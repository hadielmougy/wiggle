package com.wiggle.server.store;

/**
 * Search documents: the writes, and the reads that must see the primary. Held on a search shard.
 */
public interface SearchRepository extends SearchReads {

    /**
     * Writes a search document unless one for the same instance is newer; returns whether it wrote.
     * Held on a search shard.
     */
    boolean upsertSearchDoc(Rows.SearchDoc doc);

    /** Deletes the document. */
    void deleteSearchDoc(String instanceId);

    /** Deletes up to {@code max} documents whose instance last changed before {@code updatedBefore}. */
    int deleteSearchDocsBefore(long updatedBefore, int max);
}
