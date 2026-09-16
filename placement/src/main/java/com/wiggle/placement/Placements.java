package com.wiggle.placement;


import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * The placement decisions, as pure functions over a {@link Ring.Policy}.
 *
 * <p>There are only two questions, and they are not the same question:
 *
 * <ul>
 *   <li><b>Where may this cell mint?</b> — {@link #mintable}. A cell mints into the current epoch,
 *       for the shards that epoch's ring gives it. A ring that does not name the cell puts it on
 *       <em>standby</em>: it must mint nothing, because an id it forged would claim a placement it
 *       does not hold and would route to a cell that never had the instance.</li>
 *   <li><b>Where does this instance live?</b> — {@link #resolve}. Not the same thing: the ring says
 *       where a shard <em>belongs</em>, while the id's own label says where the instance was
 *       actually <em>written</em>. They agree whenever the minting cell owned the shard in that
 *       epoch, which is the ordinary case. They part company when a cell minted into the genesis
 *       default before a coordinator placed it — and there the label is right.</li>
 * </ul>
 *
 * <p>These lived in three places before: minting in the server, the ring in the coordinator,
 * resolution in the client. That is how the second question came to be answered with the first
 * question's rule. Stating them together is the point of this class, and the reason its tests can
 * generate ring histories rather than enumerate cases.
 */
public final class Placements {

    private Placements() {}

    /**
     * The epoch and shards {@code cellId} may mint into.
     *
     * <p>Empty shards mean standby, and standby is not the same as "no ring yet" — one is a ring
     * that deliberately excludes this cell, the other is a namespace nobody has placed. Both forbid
     * minting; only the first is a decision someone made.
     */
    public static Mintable mintable(Ring.Policy policy, String cellId) {
        if (policy == null) return new Mintable(0, List.of(), Reason.NO_RING);
        Ring.Epoch epoch = policy.current();
        if (epoch == null || epoch.ring().isEmpty()) {
            return new Mintable(policy.currentEpoch(), List.of(), Reason.NO_RING);
        }
        List<Integer> owned = new ArrayList<>();
        for (Ring.Slot s : epoch.ring()) {
            if (s.cellId().equals(cellId)) owned.add(s.shard());
        }
        if (owned.isEmpty()) return new Mintable(policy.currentEpoch(), List.of(), Reason.STANDBY);
        if (!epoch.mints()) return new Mintable(policy.currentEpoch(), List.of(), Reason.NOT_OPEN);
        return new Mintable(policy.currentEpoch(), List.copyOf(owned), Reason.OK);
    }

    /** What {@link #mintable} decided, and why — so a caller can report a useful refusal. */
    public record Mintable(long epoch, List<Integer> shards, Reason reason) {
        public boolean allowed() { return reason == Reason.OK; }
    }

    /** Why a cell may not mint. */
    public enum Reason {
        /** It owns shards in an open epoch. */                         OK,
        /** No ring names this namespace yet. */                        NO_RING,
        /** A ring exists and deliberately does not name this cell. */  STANDBY,
        /** Its epoch is draining or retired: it serves, it does not mint. */ NOT_OPEN
    }

    /**
     * The cell holding {@code id}.
     *
     * <p>The label wins when the id carries one and {@code cellIsLive} accepts it, because it records
     * where the instance was written rather than where its shard belongs. Falling back to the ring
     * when the labelled cell has no live nodes keeps a decommissioned cell resolvable the old way
     * instead of failing outright.
     *
     * @param cellIsLive whether a cell currently has nodes that can serve; the caller owns membership
     * @return the cell id, or empty when neither the label nor a ring can answer
     */
    public static Optional<String> resolve(IdCodec.Placement id, Ring.Policy policy,
                                           Predicate<String> cellIsLive) {
        if (id == null) return Optional.empty();
        if (id.hasCell() && cellIsLive != null && cellIsLive.test(id.cellId())) {
            return Optional.of(id.cellId());
        }
        return cellFor(policy, id.epoch(), (int) id.shard());
    }

    /**
     * The cell a shard belongs to in a given epoch, by that epoch's ring.
     *
     * <p>A shard the ring does not name wraps into it. That is not a rounding convenience: a ring
     * may be smaller than the shard space, and every shard still has to land somewhere stable, or an
     * instance would become unroutable the moment the ring shrank.
     */
    public static Optional<String> cellFor(Ring.Policy policy, long epoch, int shard) {
        if (policy == null) return Optional.empty();
        Ring.Epoch er = policy.epochs().get(epoch);
        if (er == null || er.ring().isEmpty()) return Optional.empty();
        List<Ring.Slot> ring = er.ring();
        for (Ring.Slot s : ring) {
            if (s.shard() == shard) return Optional.of(s.cellId());
        }
        return Optional.of(ring.get(Math.floorMod(shard, ring.size())).cellId());
    }

    /**
     * Which of a cell's own shards a new id lands on.
     *
     * <p>The counterpart to {@link #cellFor}: that maps a shard to its cell, this maps a ulid to one
     * of the shards a cell holds. The hash runs over the <em>size of the owned set</em> and indexes
     * into it, so a cell owning {@code [3, 7]} mints 3 or 7 and never 0 or 1 — getting that wrong
     * puts ids on shards the cell does not own, which resolves them to somebody else.
     *
     * <p>Refuses when the cell owns nothing. That is standby, and minting the genesis shard from it
     * would forge an id claiming a placement this cell does not hold.
     *
     * @throws IllegalStateException when {@code owned} is empty
     */
    public static int mintShard(List<Integer> owned, String ulid) {
        if (owned == null || owned.isEmpty()) {
            throw new IllegalStateException("cell is on standby (its epoch's ring does not name it); "
                    + "it is not accepting new instances until an epoch places it");
        }
        if (owned.size() == 1) return owned.get(0);
        return owned.get((int) IdCodec.shardFor(ulid, owned.size()));
    }

    /** Every cell hosting live work: any that appears in an epoch still open or draining. */
    public static List<String> activeCells(Ring.Policy policy) {
        if (policy == null) return List.of();
        List<String> out = new ArrayList<>();
        for (Ring.Epoch epoch : policy.epochs().values()) {
            if (epoch.status() == Ring.Status.RETIRED) continue;
            for (Ring.Slot s : epoch.ring()) {
                if (!out.contains(s.cellId())) out.add(s.cellId());
            }
        }
        return out;
    }
}
