package com.wiggle.server.store;

/**
 * The compensation log of compensable steps: the reads, which a read-only connection such as a
 * read replica can serve. Held on an instance shard.
 */
public interface CompensationReads {

    /** The instance's compensation log, ordered by seq ascending. */
    java.util.List<Rows.CompLog> compensationLog(String instanceId);
}
