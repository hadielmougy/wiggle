package com.wiggle.placement;

import java.util.List;
import java.util.function.Supplier;

/**
 * One cell's current placement: the epoch it mints into and the shards it owns there. Mutable --
 * a coordinator re-points a running node when the ring changes.
 *
 * <p>An id must never carry one generation's epoch with another generation's shard, so the two are
 * one immutable {@link Snapshot} behind a single volatile reference and {@link #stampFor} is the
 * only way to mint: one read fixes both.
 *
 * <p>A {@code null} shard set is the genesis fallback (epoch 0, shard 0); an <b>empty</b> set is
 * standby, where a ring exists and does not name this cell, and nothing may be minted.
 */
public final class LivePlacement {

    /** An epoch and the shards this cell owns in it, read as one unit. */
    private record Snapshot(long epoch, int[] shards) {}

    private volatile Snapshot snap;

    private volatile Runnable onStandby;
    private volatile long lastCallbackNanos;

    /** Debounce: a burst of standby mints must not stampede the callback. */
    private static final long CALLBACK_MIN_INTERVAL_NANOS = 250_000_000L;

    /** The genesis placement: epoch 0, shard 0 -- what a node mints before it hears from anyone. */
    public LivePlacement() {
        this(0, new int[]{0});
    }

    public LivePlacement(long epoch, int[] shards) {
        set(epoch, shards);
    }

    /** Re-points this cell. {@code null} shards mean genesis {@code [0]}; empty means standby. */
    public void set(long epoch, int[] shards) {
        int[] owned = shards == null ? new int[]{0} : shards.clone();
        this.snap = new Snapshot(epoch, owned);   // one volatile write: the pair swaps atomically
    }

    public void set(long epoch, List<Integer> shards) {
        set(epoch, shards == null ? null : shards.stream().mapToInt(Integer::intValue).toArray());
    }

    public long epoch() { return snap.epoch(); }

    /** Whether this cell may mint: false on standby, where a ring exists and does not name it. */
    public boolean mintable() { return snap.shards().length > 0; }

    /**
     * The epoch and shard a single new id is stamped with, read as one unit. On standby it invokes
     * the {@link #onStandby} callback once and re-reads, so a mint that raced an epoch bump
     * self-heals, then refuses.
     *
     * @throws IllegalStateException when this cell owns no shards
     */
    public Stamp stampFor(String ulid) {
        Snapshot s = snap;                        // one read fixes both fields
        if (s.shards().length == 0) {
            notifyStandby();
            s = snap;                             // the callback may have re-pointed us
        }
        return new Stamp(s.epoch(), shardOf(s, ulid));   // still standby -> mintShard throws
    }

    /**
     * A supplier of instance ids for one namespace and cell: generate a ulid, stamp it, format that
     * same ulid with that same stamp. Each id must use the stamp taken for its own ulid.
     *
     * <p>{@code ulids} supplies the token; placement never generates one and assumes nothing about
     * its shape beyond the {@link IdCodec} segment rules.
     *
     * @throws IllegalArgumentException when {@code namespace} is blank -- such an id is not routable
     */
    public Supplier<String> minter(String namespace, String cellId, Supplier<String> ulids) {
        if (namespace == null || namespace.isBlank()) {
            throw new IllegalArgumentException("an instance id needs a namespace to be routable");
        }
        if (ulids == null) throw new IllegalArgumentException("no ulid source");
        return () -> {
            String ulid = ulids.get();
            Stamp st = stampFor(ulid);
            return IdCodec.format(namespace, cellId, st.epoch(), st.shard(), ulid);
        };
    }

    /** Installs a debounced callback for when a mint finds this cell on standby. Optional. */
    public void onStandby(Runnable callback) {
        this.onStandby = callback;
    }

    private void notifyStandby() {
        Runnable r = onStandby;
        if (r == null) return;
        long now = System.nanoTime();
        if (now - lastCallbackNanos < CALLBACK_MIN_INTERVAL_NANOS) return;
        lastCallbackNanos = now;
        r.run();                                  // best-effort
    }

    /** The epoch and shard a single new id is stamped with. */
    public record Stamp(long epoch, int shard) {}

    private static int shardOf(Snapshot s, String ulid) {
        List<Integer> owned = new java.util.ArrayList<>(s.shards().length);
        for (int shard : s.shards()) owned.add(shard);
        try {
            return Placements.mintShard(owned, ulid);
        } catch (IllegalStateException e) {
            throw new IllegalStateException(e.getMessage() + " (epoch " + s.epoch() + ")", e);
        }
    }
}
