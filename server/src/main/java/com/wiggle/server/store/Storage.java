package com.wiggle.server.store;

import com.wiggle.core.ShardIds;

import java.util.List;
import java.util.function.Function;

/**
 * Storage SPI for the engine. Implementations must make each transaction atomic and must honour
 * {@link InstanceRepository#lockInstance} as a mutual-exclusion point for a single workflow instance.
 *
 * <p>A transaction runs on one shard: {@link #inShard} to write, {@link #readShard} to read. Callers open
 * them through {@link ShardTransactions}, which binds the transaction's repositories to the thread for
 * {@link Repositories}. A store over one database runs every shard's transactions on it.
 */
public interface Storage extends AutoCloseable {

    /** Applies the engine schema ({@code wf_*} tables). */
    void migrate();

    /** The shard holding the cluster-global rows: nodes, schedules, event cursors. */
    default int home() { return 0; }

    /** The shard holding accounts, roles and sessions. */
    default int auth() { return home(); }

    /** The shards that hold instances, in a stable order. */
    default List<Integer> instanceShards() { return List.of(home()); }

    /** The shard {@code id} lives on: the one it carries, or {@link #home} when it carries none. */
    default int shardOf(String id) { return ShardIds.shardOf(id).orElse(home()); }

    /** Runs {@code body} in a write transaction of its own on {@code shard}, on its connection of its own. */
    <R> R inShard(int shard, Function<WriteRepositories, R> body);

    /** Runs {@code body} in a read-only transaction of its own on {@code shard}, served by a replica when {@code freshness} allows. */
    default <R> R readShard(int shard, Freshness freshness, Function<ReadRepositories, R> body) {
        return inShard(shard, body::apply);
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
     * cross-node identity.
     */
    default String fingerprint() { return null; }

    @Override void close();
}
