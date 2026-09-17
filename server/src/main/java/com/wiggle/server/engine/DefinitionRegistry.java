package com.wiggle.server.engine;

import com.wiggle.core.ExecutionMode;
import com.wiggle.core.Node;
import com.wiggle.core.Json;
import com.wiggle.core.WorkflowDefinition;
import com.wiggle.server.store.GraphStore;
import com.wiggle.server.store.Storage;
import com.wiggle.server.store.Tx;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Owns workflow definitions. Registration stores two things: the raw submitted graph as a
 * write-once blob (source of truth for audit/replay) and a normalised set of node/edge rows
 * that drive execution.
 *
 * <p>The engine reads graphs through a bounded in-memory cache of materialised {@link LazyGraph}s,
 * loaded from those rows (never the blob) and safe to share because a (name, version) is a content
 * hash: write-once, no invalidation. Profiled before this cache existed, graph-row reads were 7.0
 * statements per step -- 40% of all engine DB traffic. Past the cap ({@code WIGGLE_GRAPH_CACHE_MAX},
 * default 256, 0 disables) a graph is served the old way, one node at a time. The blob-loading
 * {@code latest}/{@code lookup} paths remain for admin/describe calls only.
 */
public final class DefinitionRegistry {

    private final Storage storage;
    // A definition's execution mode is a single immutable enum per version -- cheap to cache,
    // unlike the whole graph. Lets the hot poll path stamp the mode without parsing the blob.
    private final ConcurrentHashMap<String, ExecutionMode> modeCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LazyGraph> graphCache = new ConcurrentHashMap<>();
    private final int graphCacheMax;

    public DefinitionRegistry(Storage storage) {
        this(storage, cacheMaxFromEnv());
    }

    DefinitionRegistry(Storage storage, int graphCacheMax) {
        this.storage = storage;
        this.graphCacheMax = graphCacheMax;
    }

    private static int cacheMaxFromEnv() {
        String v = System.getenv("WIGGLE_GRAPH_CACHE_MAX");
        if (v == null || v.isBlank()) return 256;
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            return 256;
        }
    }

    public WorkflowDefinition register(WorkflowDefinition def) {
        storage.inTxVoid(tx -> {
            tx.putDefinition(def.name(), def.version(), Json.write(def.toJson()));
            tx.putGraph(def);
        });
        modeCache.put(def.key(), def.executionMode());
        return def;
    }

    /** The execution mode for a version, cached; parses the blob at most once per version. */
    public ExecutionMode executionMode(GraphStore graphs, String name, int version) {
        return modeCache.computeIfAbsent(name + ":" + version, k ->
                graphs.definition(name, version)
                        .map(body -> WorkflowDefinition.fromJson(Json.parse(body)).executionMode())
                        .orElse(ExecutionMode.DEFAULT));
    }

    /** The engine's graph handle: registry-cached and fully materialised when the cache has room,
     *  else the per-node lazy view. Both read the same normalised rows. */
    public LazyGraph graph(GraphStore graphs, String name, int version) {
        String key = name + ":" + version;
        LazyGraph cached = graphCache.get(key);
        if (cached != null) return cached;
        if (graphCacheMax <= 0 || graphCache.size() >= graphCacheMax) {
            return new LazyGraph(graphs, name, version);
        }
        List<Node> nodes = graphs.graphNodes(name, version);
        Optional<String> start = graphs.graphStartNode(name, version);
        if (nodes.isEmpty() || start.isEmpty()) {
            return new LazyGraph(graphs, name, version);   // unknown workflow: the lazy path keeps today's errors
        }
        Map<String, Node> byId = new HashMap<>(nodes.size() * 2);
        for (Node n : nodes) byId.put(n.id(), n);
        LazyGraph g = new LazyGraph(name, version, start.get(), Map.copyOf(byId));
        graphCache.putIfAbsent(key, g);
        return g;
    }

    public WorkflowDefinition get(String name, int version) {
        return lookup(name, version).orElseThrow(
                () -> new IllegalArgumentException("no such workflow definition: " + name + ":" + version));
    }

    public Optional<WorkflowDefinition> lookup(String name, int version) {
        return storage.inTx(tx -> load(tx, name, version));
    }

    private Optional<WorkflowDefinition> load(GraphStore graphs, String name, int version) {
        return graphs.definition(name, version).map(body -> WorkflowDefinition.fromJson(Json.parse(body)));
    }

    public Optional<WorkflowDefinition> latest(String name) {
        return storage.inTx(tx -> tx.latestVersion(name).flatMap(v -> load(tx, name, v)));
    }

    public List<String> names() {
        return storage.inTx(Tx::definitionNames);
    }
}
