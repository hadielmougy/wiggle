package com.wiggle.server.store;

/**
 * The compensation log of compensable steps: the writes, and the reads that must see the primary.
 * Held on an instance shard.
 */
public interface CompensationRepository extends CompensationReads {

    void appendCompensation(Rows.CompLog entry);

    void markCompensated(String instanceId, long seq);
}
