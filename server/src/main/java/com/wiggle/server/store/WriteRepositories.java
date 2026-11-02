package com.wiggle.server.store;

/**
 * The repositories of one database, area by area: {@link ReadRepositories} with the writes and the
 * locking reads. Each runs on the transaction open on the calling thread.
 */
public interface WriteRepositories extends ReadRepositories {
    @Override ShardRepository shards();
    @Override AuthRepository auth();
    @Override SearchRepository search();
    @Override InstanceRepository instances();
    @Override TokenRepository tokens();
    @Override ScheduleRepository schedules();
    @Override ClusterRepository cluster();
    @Override CompensationRepository compensation();
    @Override EventRepository events();
    @Override GraphStore graph();
    DynamicNodeRepository dynamicNodes();
    TaskLocks locks();

    /**
     * From here until the transaction commits, its token and instance writes may be held back in a
     * {@link WriteBuffer} and written in bulk, before the next statement or at commit.
     */
    void bufferWrites();
}
