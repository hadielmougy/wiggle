package com.wiggle.server.store;

/**
 * The reads of one database, area by area: everything a caller may do on a read-only connection,
 * such as a read replica. Each runs on the transaction open on the calling thread.
 */
public interface ReadRepositories {
    ShardReads shards();
    AuthReads auth();
    SearchReads search();
    InstanceReads instances();
    TokenReads tokens();
    ScheduleReads schedules();
    ClusterReads cluster();
    CompensationReads compensation();
    EventReads events();
    GraphReads graph();
}
