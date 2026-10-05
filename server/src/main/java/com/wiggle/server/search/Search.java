package com.wiggle.server.search;

import com.wiggle.server.auth.Scope;
import com.wiggle.server.engine.EngineException;
import com.wiggle.server.engine.WorkflowEngine;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.Storage;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

    /** The hits, best first, whether a search shard could not answer, and the model a semantic search used. */
    public record SemanticResult(List<Hit> hits, boolean partial, String model) { }

    private final SearchIndex index;
    private final WorkflowEngine engine;
    private final Storage storage;
    private final Map<String, Embedder> embedders = new LinkedHashMap<>();

    public Search(SearchIndex index, WorkflowEngine engine) {
        this(index, engine, null, List.of());
    }

    /** @param embedders the models this node can embed a query with; none disables semantic search */
    public Search(SearchIndex index, WorkflowEngine engine, Storage storage, List<Embedder> embedders) {
        this.index = index;
        this.engine = engine;
        this.storage = storage;
        embedders.forEach(e -> this.embedders.put(e.model(), e));
    }

    /** Whether this node can run a semantic search at all: it has an embedder. */
    public boolean semanticEnabled() {
        return !embedders.isEmpty();
    }

    /**
     * The instances whose meaning is closest to {@code text}, by the newest model whose index is
     * complete, under the same filters and scoping as {@link #search}. Fails as a precondition when
     * no model's index is complete yet, or when this node has no embedder for the one that is.
     */
    public SemanticResult semantic(String text, String workflow, String status, Long from, Long to, int limit,
                                   boolean partialOk, Scope readable) {
        if (embedders.isEmpty()) {
            throw EngineException.conflict("semantic search needs an embedder: set WIGGLE_EMBEDDER");
        }
        Rows.SearchModel serving = storage.inHome(tx -> tx.searchModels()).stream()
                .filter(m -> m.state().equals(Rows.SearchModel.READY))
                .max(Comparator.comparingLong(Rows.SearchModel::startedAt))
                .orElseThrow(() -> EngineException.conflict("the vector index is still being built; "
                        + "semantic search starts when it is complete"));
        Embedder embedder = embedders.get(serving.model());
        if (embedder == null) {
            throw EngineException.conflict("queries are served by model " + serving.model() + ", which this node "
                    + "has no embedder for; configure it (WIGGLE_EMBEDDER_PREVIOUS_MODEL) until the new index is built");
        }
        Scope workflows = (readable == null ? Scope.ALL : readable).narrow(workflow);
        if (workflows.isEmpty()) return new SemanticResult(List.of(), false, serving.model());
        float[] vector = embedder.embed(List.of(text == null ? "" : text)).getFirst();
        SearchIndex.Result r = index.searchVectors(new Rows.VectorQuery(serving.model(), vector, workflows, status,
                from, to, Math.max(1, Math.min(limit, 1000))), partialOk);
        return new SemanticResult(hits(r), r.partial(), serving.model());
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
        Scope workflows = (readable == null ? Scope.ALL : readable).narrow(workflow);
        if (workflows.isEmpty()) return new Result(List.of(), false);
        SearchIndex.Result r = index.search(new Rows.SearchQuery(text, workflows, status, from, to,
                Math.max(1, Math.min(limit, 1000))), partialOk);
        return new Result(hits(r), r.partial());
    }
}
