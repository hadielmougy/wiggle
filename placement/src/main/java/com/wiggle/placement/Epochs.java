package com.wiggle.placement;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The epoch lifecycle: opening a new placement, draining the old one, retiring it once empty.
 *
 * <p>Split in two on purpose. The <b>transitions</b> ({@link #opening}, {@link #withStatus}) are
 * pure functions from one policy to the next, so they can be generated against and reasoned about
 * without a database. The <b>operations</b> ({@link #openEpoch}, {@link #retire}) add the
 * compare-and-set retry that makes a transition safe when several coordinators share a store.
 *
 * <p>That split is not tidiness. The transition is where the rules live — a new epoch is
 * {@code current + 1}, the previous one becomes DRAINING and keeps its ring forever, a retired epoch
 * is never reopened — and those rules were previously expressible only through a store, which is why
 * they were tested by starting a coordinator.
 */
public final class Epochs {

    /** How many times an operation re-reads and retries before giving up on a contended store. */
    public static final int CAS_ATTEMPTS = 5;

    private Epochs() {}

    // ---------------------------------------------------------------- transitions (pure)

    /**
     * The policy that results from publishing {@code ring} as a new epoch.
     *
     * <p>A namespace with no policy starts at epoch 0. Otherwise the epoch advances by one, the
     * outgoing epoch moves to DRAINING — keeping its ring, because ids minted into it must stay
     * resolvable for as long as their instances live — and every older epoch is left exactly as it
     * was, including retired ones.
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

    /**
     * The policy with one epoch moved to {@code status}.
     *
     * <p>Only forwards: OPEN → DRAINING → RETIRED. Going back would resurrect a placement that work
     * has already drained away from, so it is refused rather than ignored — a caller asking for it
     * has misunderstood something, and silently doing nothing would hide that.
     */
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
        if (epoch == policy.currentEpoch() && status == Ring.Status.RETIRED) {
            throw new IllegalArgumentException("epoch " + epoch + " of '" + policy.namespace()
                    + "' is the one new ids are minted into; open a new epoch before retiring it");
        }
        Map<Long, Ring.Epoch> epochs = new LinkedHashMap<>(policy.epochs());
        epochs.put(epoch, new Ring.Epoch(current.ring(), status));
        return new Ring.Policy(policy.namespace(), policy.currentEpoch(), epochs);
    }

    // ---------------------------------------------------------------- operations (store + retry)

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

            // recomputed from THIS attempt's read: a transition built from a stale policy would
            // reopen an epoch someone else just drained
            Ring.Policy updated = transition.apply(previous);
            if (store.compareAndSet(namespace, revision, updated)) return updated;
        }
        throw new IllegalStateException(what + ": '" + namespace + "' stayed contended for "
                + CAS_ATTEMPTS + " attempts");
    }
}
