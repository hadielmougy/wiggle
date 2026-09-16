package com.wiggle.placement;

import java.util.List;
import java.util.function.Supplier;

/**
 * One cell's <em>current</em> placement: the epoch it mints into and the shards it owns there.
 *
 * <p>Mutable, unlike everything else here, because a coordinator re-points a running node when the
 * ring changes and restarting it to learn that would defeat the point. What makes it a placement
 * concern rather than server plumbing is the invariant it exists to hold:
 *
 * <p><b>An id must never carry one generation's epoch with another generation's shard.</b> Read the
 * epoch and the shard separately and a re-point can interleave between them, stamping a pair that
 * was never true — and that id then resolves, through the ring of an epoch that did not own its
 * shard, to a cell which has never held the instance. So the two are one immutable
 * {@link Snapshot} behind a single volatile reference, and {@link #stampFor} is the only way to
 * mint: one read fixes both.
 *
 * <p>The other rule it carries is the difference between two empty-looking states. A {@code null}
 * shard set is the <em>genesis</em> fallback — a node that has not yet heard from a coordinator,
 * minting exactly what it did before rings existed. An <em>empty</em> set is <em>standby</em>: a
 * ring exists and deliberately does not name this cell, so it must mint nothing. Collapsing them
 * would let a standby cell forge genesis ids that belong to somebody else. Those are the same two
 * answers {@link Placements#mintable} gives as {@code NO_RING} and {@code STANDBY}.
 *
 * <p>Nothing here knows a coordinator exists. {@link #onStandby(Runnable)} takes a callback the host
 * supplies; this class debounces it and never inspects it.
 */
public final class LivePlacement {

    /** An epoch and the shards this cell owns in it, as one unit so a mint reads a consistent pair. */
    private record Snapshot(long epoch, int[] shards) {}

    private volatile Snapshot snap;

    private volatile Runnable onStandby;
    private volatile long lastCallbackNanos;

    /** A burst of starts against a standby cell must not stampede whatever the callback talks to. */
    private static final long CALLBACK_MIN_INTERVAL_NANOS = 250_000_000L;

    /** The genesis placement: epoch 0, shard 0 -- what a node mints before it hears from anyone. */
    public LivePlacement() {
        this(0, new int[]{0});
    }

    public LivePlacement(long epoch, int[] shards) {
        set(epoch, shards);
    }

    /**
     * Re-points this cell. {@code null} shards mean the genesis fallback {@code [0]}; an
     * <b>empty</b> array means standby, and the two are kept distinct so standby never degrades into
     * minting the genesis shard.
     */
    public void set(long epoch, int[] shards) {
        int[] owned = shards == null ? new int[]{0} : shards.clone();
        this.snap = new Snapshot(epoch, owned);   // single volatile write -> the pair swaps atomically
    }

    public void set(long epoch, List<Integer> shards) {
        set(epoch, shards == null ? null : shards.stream().mapToInt(Integer::intValue).toArray());
    }

    public long epoch() { return snap.epoch(); }

    /** Whether this cell may mint: false on standby, where a ring exists and does not name it. */
    public boolean mintable() { return snap.shards().length > 0; }

    /**
     * The epoch and shard a single new id is stamped with, read as one unit.
     *
     * <p>On standby it gives the host one chance to catch up — a start that raced an epoch bump
     * self-heals instead of failing — and then refuses, because a standby cell minting anything is
     * the failure this whole class is arranged to prevent.
     *
     * @throws IllegalStateException when this cell owns no shards
     */
    public Stamp stampFor(String ulid) {
        Snapshot s = snap;                        // one volatile read fixes both fields for this id
        if (s.shards().length == 0) {
            notifyStandby();
            s = snap;                             // the callback may have re-pointed us
        }
        return new Stamp(s.epoch(), shardOf(s, ulid));   // still standby -> mintShard throws
    }

    /**
     * A supplier of well-formed instance ids for one namespace and cell.
     *
     * <p>Minting is three steps in a fixed order — generate the ulid, stamp it, format the result —
     * and the order is the whole point. The stamp has to be taken <em>for the ulid being minted</em>
     * and used for that same id: take the stamp first and format with a re-read epoch, or stamp one
     * ulid and format another, and the id claims a placement that was never true. That is the tear
     * {@link #stampFor} exists to prevent, and leaving the sequence for each caller to assemble is
     * how it gets reintroduced.
     *
     * <p>{@code ulids} supplies the opaque token. Placement never generates one and makes no
     * assumption about its shape beyond the {@link IdCodec} character rules — the shard is a hash of
     * whatever arrives, so the token primitive stays the host's choice.
     *
     * @throws IllegalArgumentException when {@code namespace} is blank; an id with no namespace is
     *         not routable, and minting one is a decision this class will not make silently
     */
    public Supplier<String> minter(String namespace, String cellId, Supplier<String> ulids) {
        if (namespace == null || namespace.isBlank()) {
            throw new IllegalArgumentException("an instance id needs a namespace to be routable");
        }
        if (ulids == null) throw new IllegalArgumentException("no ulid source");
        return () -> {
            String ulid = ulids.get();
            Stamp st = stampFor(ulid);            // atomic (epoch, shard) for THIS ulid
            return IdCodec.format(namespace, cellId, st.epoch(), st.shard(), ulid);
        };
    }

    /**
     * Installs a callback invoked when a mint finds this cell on standby, debounced. The host decides
     * what it does — typically re-fetching placement from wherever it came from. Optional: a cell
     * that nobody re-points never sets one.
     */
    public void onStandby(Runnable callback) {
        this.onStandby = callback;
    }

    private void notifyStandby() {
        Runnable r = onStandby;
        if (r == null) return;
        long now = System.nanoTime();
        if (now - lastCallbackNanos < CALLBACK_MIN_INTERVAL_NANOS) return;
        lastCallbackNanos = now;
        r.run();                                  // best-effort; the host owns its failure modes
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
