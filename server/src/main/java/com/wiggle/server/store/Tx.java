package com.wiggle.server.store;

import com.wiggle.server.store.Rows.Instance;

import java.util.Optional;

/**
 * The mutable runtime state of one transaction: instances, tokens, leases, schedules, and cluster
 * nodes. It is every repository at once: {@link ReadTx}'s reads, the writes and locking reads of each
 * repository, and {@link GraphStore} -- the immutable definition/graph reference data -- so a single
 * transaction can read the graph and mutate runtime state atomically. Every engine mutation runs
 * inside a {@link Storage} transaction.
 */
public interface Tx extends ReadTx, GraphStore,
        ShardRepository,
        AuthRepository,
        SearchRepository,
        InstanceRepository,
        TokenRepository,
        ScheduleRepository,
        ClusterRepository,
        CompensationRepository,
        EventRepository,
        DynamicNodeRepository {

    /**
     * Whether a throw rolls this transaction's writes back. The in-memory store answers false:
     * it applies writes directly and cannot undo them. Write-buffering keys off this -- deferring
     * writes that a rollback cannot reclaim would let a mid-transaction throw discard the buffer
     * while already-issued writes stand, leaving a state no execution could have produced.
     */
    default boolean transactional() { return true; }

    /** {@code lockInstance} on the instance that owns token {@code tokenId}; empty when either the
     *  token or its instance is missing. */
    default Optional<Instance> lockInstanceOf(String tokenId) {
        return findToken(tokenId).flatMap(t -> lockInstance(t.instanceId));
    }

    /** {@code lockInstanceOf}, and token {@code tokenId} read under that lock; empty when either the
     *  token or its instance is missing. */
    default Optional<Rows.LockedTask> lockTask(String tokenId) {
        return lockInstanceOf(tokenId).flatMap(i -> findToken(tokenId).map(t -> new Rows.LockedTask(i, t)));
    }
}
