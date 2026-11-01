package com.wiggle.server.store;

/**
 * The reads of one transaction: every repository's reads, which is everything a caller may do on a
 * read-only connection, such as a read replica. {@link Tx} extends it with the writes and the locking
 * reads, so a body typed as {@code ReadTx} cannot write.
 */
public interface ReadTx extends GraphReads,
        ShardReads,
        AuthReads,
        SearchReads,
        InstanceReads,
        TokenReads,
        ScheduleReads,
        ClusterReads,
        CompensationReads,
        EventReads {
}
