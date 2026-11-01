package com.wiggle.server.store;

import java.util.List;

/**
 * Search documents: the reads, which a read-only connection such as a read replica can serve. Held
 * on a search shard.
 */
public interface SearchReads {

    /** The best {@code query.limit()} matches on this shard, filters applied, best first. Held on a search shard. */
    List<Rows.SearchHit> searchDocs(Rows.SearchQuery query);

    /** Up to {@code max} documents with an instance id after {@code afterId}, in id order. Held on a search shard. */
    List<Rows.SearchDoc> searchDocsAfter(String afterId, int max);
}
