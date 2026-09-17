package com.wiggle.placement;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The epoch lifecycle: opening a new placement, draining the old one, retiring it once empty.
 *
 * <p>The <b>transitions</b> ({@link #opening}, {@link #withStatus}) are pure functions from one
 * policy to the next. The <b>operations</b> ({@link #openEpoch}, {@link #retire}) add the
 * compare-and-set retry that makes a transition safe when several coordinators share a store.
 */
public final class Epochs {

    /** How many times an operation re-reads and retries before giving up on a contended store. */
    public static final int CAS_ATTEMPTS = 5;

    private Epochs() {}

    /**
     * The policy that results from publishing {@code ring} as a new epoch. A namespace with no
     * policy starts at epoch 0; otherwise the epoch advances by one and the outgoing epoch moves to
     * DRAINING, keeping its ring so ids minted into it stay resolvable. Older epochs are untouched.
     */
    public static Ring.Policy opening(String namespace, Ring.Policy previous, List<Ring.Slot> ring) {
        if (ring == null || ring.isEmpty()) {
            throw new IllegalArgumentException("an epoch must place at least one shard on a cell");
        }
        if (previous == null) {
            return new Ring.Policy(namespace, 0, Map.of(0L, new Ring.Epoch(ring, Ring.Status.OPEN)));
        }
        long next = previous.currentEpoch() + 1;
        Map<Long, Ring.Epoch> epochs = new LinkedHashMap<>(previous.epochs());
        Ring.Epoch outgoing = epochs.get(previous.currentEpoch());
        if (outgoing != null && outgoing.status() == Ring.Status.OPEN) {
            epochs.put(previous.currentEpoch(), new Ring.Epoch(outgoing.ring(), Ring.Status.DRAINING));
        }
        epochs.put(next, new Ring.Epoch(ring, Ring.Status.OPEN));
        return new Ring.Policy(namespace, next, epochs);
    }

    /** The policy with one epoch moved to {@code status}. Only forwards: OPEN → DRAINING → RETIRED. */
    public static Ring.Policy withStatus(Ring.Policy policy, long epoch, Ring.Status status) {
        if (policy == null) throw new IllegalArgumentException("no policy to change");
        Ring.Epoch current = policy.epochs().get(epoch);
        if (current == null) {
            throw new IllegalArgumentException("namespace '" + policy.namespace()
                    + "' has no epoch " + epoch);
        }
        if (status.ordinal() < current.status().ordinal()) {
            throw new IllegalArgumentException("epoch " + epoch + " of '" + policy.namespace()
                    + "' is " + current.status() + "; it cannot go back to " + status);
        }
        if (policy.isCurrentEpoch(epoch) && status == Ring.Status.RETIRED) {
            throw new IllegalArgumentException("epoch " + epoch + " of '" + policy.namespace()
                    + "' is the one new ids are minted into; open a new epoch before retiring it");
        }
        Map<Long, Ring.Epoch> epochs = new LinkedHashMap<>(policy.epochs());
        epochs.put(epoch, new Ring.Epoch(current.ring(), status));
        return new Ring.Policy(policy.namespace(), policy.currentEpoch(), epochs);
    }

    /**
     * Publishes {@code ring} as a new epoch, retrying on a lost compare-and-set.
     *
     * @return the policy as written
     * @throws IllegalStateException when the store stays contended for {@link #CAS_ATTEMPTS} tries
     */
    public static Ring.Policy openEpoch(PlacementStore store, String namespace, List<Ring.Slot> ring) {
        return update(store, namespace, previous -> opening(namespace, previous, ring), "openEpoch");
    }

    /** Moves an epoch to RETIRED once its work has drained, retrying on a lost compare-and-set. */
    public static Ring.Policy retire(PlacementStore store, String namespace, long epoch) {
        return update(store, namespace, previous -> withStatus(previous, epoch, Ring.Status.RETIRED),
                "retire");
    }

    /** Applies a transition under compare-and-set, re-reading and recomputing on each attempt. */
    private static Ring.Policy update(PlacementStore store, String namespace,
                                      java.util.function.UnaryOperator<Ring.Policy> transition,
                                      String what) {
        for (int attempt = 0; attempt < CAS_ATTEMPTS; attempt++) {
            Optional<PlacementStore.Versioned> current = store.get(namespace);
            Ring.Policy previous = current.map(PlacementStore.Versioned::policy).orElse(null);
            long revision = current.map(PlacementStore.Versioned::revision).orElse(0L);
            Ring.Policy updated = transition.apply(previous);
            if (store.compareAndSet(namespace, revision, updated)) return updated;
        }
        throw new IllegalStateException(what + ": '" + namespace + "' stayed contended for "
                + CAS_ATTEMPTS + " attempts");
    }
}
