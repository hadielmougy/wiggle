package com.wiggle.server.search;

import com.wiggle.server.auth.Scope;
import com.wiggle.server.engine.EngineException;
import com.wiggle.server.engine.WorkflowEngine;
import com.wiggle.server.store.Rows;

import java.util.List;

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

    private List<Hit> hits(SearchIndex.Result r) {
        return r.hits().stream().map(h -> {
            Rows.SearchDoc d = h.doc();
            return new Hit(d.instanceId(), d.workflow(), d.version(), d.status(), d.correlationId(), d.createdAt(),
                    d.updatedAt(), h.score(), engine.instance(d.instanceId()).isEmpty());
        }).toList();
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
                         boolean partialOk, Scope readable) {
        return search(text, workflow, status, from, to, limit, partialOk, readable, List.of());
    }

    /**
     * {@link #search(String, String, String, Long, Long, int, boolean, Scope)}, keeping the instances
     * whose search attributes satisfy every filter. Attributes are declared per flow, so the filters
     * need {@code workflow}.
     */
    public Result search(String text, String workflow, String status, Long from, Long to, int limit,
                         boolean partialOk, Scope readable, List<Rows.AttrFilter> attributes) {
        if (!attributes.isEmpty() && workflow == null) {
            throw EngineException.badRequest("attribute filters need a workflow: search attributes are declared per flow");
        }
        Scope workflows = (readable == null ? Scope.ALL : readable).narrow(workflow);
        if (workflows.isEmpty()) return new Result(List.of(), false);
        SearchIndex.Result r = index.search(new Rows.SearchQuery(text, workflows, status, from, to,
                Math.max(1, Math.min(limit, 1000)), attributes), partialOk);
        return new Result(hits(r), r.partial());
    }
}
