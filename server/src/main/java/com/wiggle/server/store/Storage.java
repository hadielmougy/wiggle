package com.wiggle.server.store;

import com.wiggle.core.ShardIds;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Storage SPI for the engine. Implementations must make each transaction atomic and must honour
 * {@link Tx#lockInstance} as a mutual-exclusion point for a single workflow instance.
 *
 * <p>The engine opens transactions through the routed entry points: {@link #inTxFor} for the shard an
 * instance or token id carries, {@link #inShard} for one shard by number, {@link #inHome} for the
 * cluster-global rows, and {@link #readShard} / {@link #readFor} for reads that never write. A store
 * over one database answers all of them with {@link #inTx}.
 */
public interface Storage extends AutoCloseable {

    /** Applies the engine schema ({@code wf_*} tables). */
    void migrate();

    /** An unrouted transaction on a store with one database. */
    <R> R inTx(Function<Tx, R> work);

    default void inTxVoid(Consumer<Tx> work) {
        inTx(tx -> { work.accept(tx); return null; });
    }

    /** The shard holding the cluster-global rows: nodes, schedules, event cursors. */
    default int home() { return 0; }

    /** The shards that hold instances, in a stable order. */
    default List<Integer> instanceShards() { return List.of(home()); }

    /** The shard {@code id} lives on: the one it carries, or {@link #home} when it carries none. */
    default int shardOf(String id) { return ShardIds.shardOf(id).orElse(home()); }

    /** A transaction on one shard. */
    default <R> R inShard(int shard, Function<Tx, R> work) { return inTx(work); }

    /** A transaction on the shard that holds the instance or token {@code id}. */
    default <R> R inTxFor(String id, Function<Tx, R> work) { return inShard(shardOf(id), work); }

    /** A transaction on the {@link #home} shard. */
    default <R> R inHome(Function<Tx, R> work) { return inShard(home(), work); }

    /** A read-only transaction on one shard, served by a replica when {@code freshness} allows. */
    default <R> R readShard(int shard, Freshness freshness, Function<ReadTx, R> work) {
        return inShard(shard, work::apply);
    }

    /** {@link #readShard} on the shard that holds the instance or token {@code id}. */
    default <R> R readFor(String id, Freshness freshness, Function<ReadTx, R> work) {
        return readShard(shardOf(id), freshness, work);
    }

    /** Whether any shard has read replicas, so something must keep their lag measured. */
    default boolean hasReplicas() { return false; }

    /** Stamps the replica-lag heartbeat on every primary that has replicas. The leader calls it about
     *  once a second. */
    default void beatPrimaries(long now) { }

    /** Re-measures every replica's lag and health. Every node calls it about once a second. */
    default void probeReplicas(long now) { }

    /**
     * A stable identity of the underlying store: the same for every node pointed at the same
     * database, and different across databases. Returns {@code null} when the backend has no
     * cross-node identity (e.g. in-memory, which cannot be shared between processes).
     */
    default String fingerprint() { return null; }

    @Override void close();
}
