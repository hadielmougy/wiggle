package com.wiggle.placement;


import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * The placement decisions, as pure functions over a {@link Ring.Policy}. Two distinct questions:
 *
 * <ul>
 *   <li><b>Where may this cell mint?</b> — {@link #mintable}, from the current epoch's ring.</li>
 *   <li><b>Where does this instance live?</b> — {@link #resolve}. The ring says where a shard
 *       <em>belongs</em>; the id's own label says where the instance was <em>written</em>. They
 *       differ when a cell minted into the genesis default before a coordinator placed it, and
 *       there the label is right.</li>
 * </ul>
 */
public final class Placements {

    private Placements() {}

    /**
     * The epoch and shards {@code cellId} may mint into. Empty shards mean it may not mint; the
     * {@link Reason} separates standby (a ring excludes it) from no ring at all.
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
        /** A ring exists and does not name this cell. */               STANDBY,
        /** Its epoch is draining or retired: it serves, it does not mint. */ NOT_OPEN
    }

    /**
     * The cell holding {@code id}. The id's own cell label wins when it has one and that cell is
     * live; otherwise the epoch's ring answers, so a decommissioned cell stays resolvable.
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
     * The cell a shard belongs to in a given epoch, by that epoch's ring. A ring may be smaller than
     * the shard space, so a shard it does not name wraps into it rather than becoming unroutable.
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
     * Which of a cell's own shards a new id lands on. The hash runs over the <em>size</em> of the
     * owned set and indexes into it, so a cell owning {@code [3, 7]} mints 3 or 7, never 0 or 1.
     *
     * @throws IllegalStateException when {@code owned} is empty -- that is standby, which mints nothing
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
