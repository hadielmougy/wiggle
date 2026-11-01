package com.wiggle.server.store;

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
        DynamicNodeRepository,
        TaskLocks {

    /**
     * Whether a throw rolls this transaction's writes back. The in-memory store answers false:
     * it applies writes directly and cannot undo them. Write-buffering keys off this -- deferring
     * writes that a rollback cannot reclaim would let a mid-transaction throw discard the buffer
     * while already-issued writes stand, leaving a state no execution could have produced.
     */
    default boolean transactional() { return true; }
}
