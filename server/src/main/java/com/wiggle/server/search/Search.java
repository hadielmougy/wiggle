package com.wiggle.server.search;

import com.wiggle.server.engine.WorkflowEngine;
import com.wiggle.server.store.Rows;

import java.util.List;
import java.util.Set;

/**
 * Full-text search over instances, as callers use it: limited to the workflows the caller may read,
 * and with each hit marked purged when its instance is gone while its document lives on.
 */
public final class Search {

    /** One hit: the instance as it was last indexed. */
    public record Hit(String instanceId, String workflow, int version, String status, String correlationId,
                      long createdAt, long updatedAt, double score, boolean purged) { }

    /** The hits, best first, and whether a search shard could not answer. */
    public record Result(List<Hit> hits, boolean partial) { }

    private final SearchIndex index;
    private final WorkflowEngine engine;

    public Search(SearchIndex index, WorkflowEngine engine) {
        this.index = index;
        this.engine = engine;
    }

    public SearchIndex index() {
        return index;
    }

    /**
     * @param workflow  only this workflow, or null for any
     * @param readable  the workflows the caller may read, or null for every one
     * @param partialOk answer from the shards that respond when one does not, marking the result partial
     */
    public Result search(String text, String workflow, String status, Long from, Long to, int limit,
                         boolean partialOk, Set<String> readable) {
        Set<String> workflows;
        if (workflow == null) workflows = readable;
        else if (readable == null || readable.contains(workflow)) workflows = Set.of(workflow);
        else workflows = Set.of();
        if (workflows != null && workflows.isEmpty()) return new Result(List.of(), false);
        SearchIndex.Result r = index.search(new Rows.SearchQuery(text, workflows, status, from, to,
                Math.max(1, Math.min(limit, 1000))), partialOk);
        List<Hit> hits = r.hits().stream().map(h -> {
            Rows.SearchDoc d = h.doc();
            return new Hit(d.instanceId(), d.workflow(), d.version(), d.status(), d.correlationId(), d.createdAt(),
                    d.updatedAt(), h.score(), engine.instance(d.instanceId()).isEmpty());
        }).toList();
        return new Result(hits, r.partial());
    }
}
