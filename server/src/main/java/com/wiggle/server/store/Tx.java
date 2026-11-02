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
     * From here until the transaction commits, its token and instance writes may be held back in a
     * {@link WriteBuffer} and written in bulk. A store whose writes a throw cannot roll back ignores
     * it: deferring them would let a mid-transaction throw discard the buffer while writes already
     * issued stand, leaving a state no execution could have produced.
     */
    default void bufferWrites() { }
}
