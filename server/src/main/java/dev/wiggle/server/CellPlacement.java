package dev.wiggle.server;

import dev.wiggle.core.IdCodec;

import java.util.List;

/**
 * A coordinator-managed cell's live placement: the epoch it mints new instance ids into and the shards
 * its cell owns in that epoch. It is mutable and thread-safe so the coordinator link can re-point a
 * running node when the policy changes (an epoch bump drains the old epoch, T12 increment 3) without
 * restarting it. A standalone (non-coordinated) cell never has one.
 *
 * <p>Defaults to epoch 0, shard {@code [0]} -- the single implicit cell -- so a node that has not yet
 * heard from the coordinator mints exactly the ids it did before rings existed (R1).
 */
public final class CellPlacement {

    private volatile long epoch;
    private volatile int[] shards;

    public CellPlacement() {
        this(0, new int[]{0});
    }

    public CellPlacement(long epoch, int[] shards) {
        set(epoch, shards);
    }

    /** Re-points this node's placement; an empty/null shard set falls back to the genesis shard {@code [0]}. */
    public void set(long epoch, int[] shards) {
        this.epoch = epoch;
        this.shards = shards == null || shards.length == 0 ? new int[]{0} : shards.clone();
    }

    public void set(long epoch, List<Integer> shards) {
        set(epoch, shards == null ? null : shards.stream().mapToInt(Integer::intValue).toArray());
    }

    public long epoch() { return epoch; }

    /** The shard to stamp on a new id: one of this cell's shards, chosen by the id's own ulid so the
     *  spread is stable and even. */
    public int shardFor(String ulid) {
        int[] s = shards;
        if (s.length == 1) return s[0];
        return s[(int) IdCodec.shardFor(ulid, s.length)];
    }
}
