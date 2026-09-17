package com.wiggle.server.engine;

import com.wiggle.core.Node;
import com.wiggle.server.store.GraphStore;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A memory-thrifty view over a compiled graph. Where {@link com.wiggle.core.WorkflowDefinition}
 * holds every node in memory, this fetches one node's neighbourhood at a time from the
 * normalised graph tables, so a huge topology never has to be materialised whole just to
 * advance a single token.
 *
 * <p>Scoped to one transaction / one {@code drive} loop and used single-threaded, so the
 * bounded LRU below needs no synchronisation; it just keeps a hot {@code drive} from
 * re-querying the same handful of nodes.
 *
 * <p>The registry's cache uses the second constructor instead: every node preloaded once from the
 * same normalised rows, immutable, shared across transactions and threads (the LRU is never
 * touched in that mode).
 */
public final class LazyGraph {

    private static final int CACHE_MAX = 64;

    private final GraphStore graphs;
    private final String name;
    private final int version;
    private final Map<String, Node> lru = new LinkedHashMap<>(16, 0.75f, true) {
        @Override protected boolean removeEldestEntry(Map.Entry<String, Node> e) {
            return size() > CACHE_MAX;
        }
    };
    private String startNode; // resolved lazily; only lifecycle start needs it
    /** Null in lazy mode; the complete, immutable node set when registry-cached. */
    private final Map<String, Node> all;

    LazyGraph(GraphStore graphs, String name, int version) {
        this.graphs = graphs;
        this.name = name;
        this.version = version;
        this.all = null;
    }

    LazyGraph(String name, int version, String startNode, Map<String, Node> nodes) {
        this.graphs = null;
        this.name = name;
        this.version = version;
        this.startNode = startNode;
        this.all = nodes;
    }

    public String name() { return name; }

    public int version() { return version; }

    public String key() { return name + ":" + version; }

    public String startNode() {
        if (startNode == null) {
            startNode = graphs.graphStartNode(name, version).orElseThrow(
                    () -> new IllegalArgumentException("no such workflow definition: " + key()));
        }
        return startNode;
    }

    public Node node(String id) {
        if (all != null) {
            Node n = all.get(id);
            if (n == null) throw new IllegalStateException("unknown node '" + id + "' in workflow " + name);
            return n;
        }
        Node cached = lru.get(id);
        if (cached != null) return cached;
        Node n = graphs.graphNode(name, version, id).orElseThrow(
                () -> new IllegalStateException("unknown node '" + id + "' in workflow " + name));
        lru.put(id, n);
        return n;
    }
}
