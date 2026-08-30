package com.wiggle.server;

import com.wiggle.core.IdCodec;

import java.util.List;

/**
 * A coordinator-managed cell's live placement: the epoch it mints new instance ids into and the shards
 * its cell owns in that epoch. It is mutable and thread-safe so the coordinator link can re-point a
 * running node when the policy changes (an epoch bump drains the old epoch, T12 increment 3) without
 * restarting it. A standalone (non-coordinated) cell never has one.
 *
 * <p>Defaults to epoch 0, shard {@code [0]} -- the single implicit cell -- so a node that has not yet
 * heard from the coordinator mints exactly the ids it did before rings existed (R1).
 *
 * <p>The epoch and its shard set are held as one immutable {@link Snapshot} behind a single
 * {@code volatile} reference, so a re-point ({@link #set}) that races a concurrent mint can never split
 * an id's epoch from its shard. A mint must read the pair atomically via {@link #stampFor}: reading
 * {@link #epoch} and {@link #shardFor} separately could interleave a {@code set} between them and stamp
 * one generation's epoch with another's shard -- which, after a reshard, mis-routes the new instance.
 */
public final class CellPlacement {

    /** An epoch and the shards this cell owns in it, as one unit so a mint reads a consistent pair. */
    private record Snapshot(long epoch, int[] shards) {}

    private volatile Snapshot snap;

    public CellPlacement() {
        this(0, new int[]{0});
    }

    public CellPlacement(long epoch, int[] shards) {
        set(epoch, shards);
    }

    /** Re-points this node's placement; an empty/null shard set falls back to the genesis shard {@code [0]}. */
    public void set(long epoch, int[] shards) {
        int[] owned = shards == null || shards.length == 0 ? new int[]{0} : shards.clone();
        this.snap = new Snapshot(epoch, owned);   // single volatile write -> the pair swaps atomically
    }

    public void set(long epoch, List<Integer> shards) {
        set(epoch, shards == null ? null : shards.stream().mapToInt(Integer::intValue).toArray());
    }

    public long epoch() { return snap.epoch(); }

    /** The shard to stamp on a new id: one of this cell's shards, chosen by the id's own ulid so the
     *  spread is stable and even. */
    public int shardFor(String ulid) {
        return shardOf(snap, ulid);
    }

    /**
     * Atomically snapshots the (epoch, shard) a new id should carry, so a concurrent {@link #set} cannot
     * split them across generations. Callers minting an id must use this rather than {@link #epoch} plus
     * {@link #shardFor}, which are two separate reads.
     */
    public Stamp stampFor(String ulid) {
        Snapshot s = snap;                        // one volatile read fixes both fields for this id
        return new Stamp(s.epoch(), shardOf(s, ulid));
    }

    /** The epoch and shard a single new id is stamped with. */
    public record Stamp(long epoch, int shard) {}

    private static int shardOf(Snapshot s, String ulid) {
        int[] sh = s.shards();
        return sh.length == 1 ? sh[0] : sh[(int) IdCodec.shardFor(ulid, sh.length)];
    }
}
