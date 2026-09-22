package com.wiggle.server.engine;

import com.wiggle.core.ExecutionMode;
import com.wiggle.core.Json;
import com.wiggle.core.Node;
import com.wiggle.core.WorkflowDefinition;
import com.wiggle.server.store.GraphStore;
import com.wiggle.server.store.Storage;
import com.wiggle.server.store.Tx;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Owns workflow definitions. Registration stores two things: the raw submitted graph as a
 * write-once blob (source of truth for audit/replay) and a normalised set of node/edge rows
 * the engine reads one node at a time. The hot path never materialises a whole graph, so no
 * full-graph cache lives here -- that in-memory copy was exactly the pressure we shed. The
 * blob-loading {@code latest}/{@code lookup} paths remain for admin/describe calls only.
 *
 * <p>Versions are declared by the author, not derived, so this is where a published version is
 * held immutable: {@link #register} compares the submitted graph's fingerprint against the stored
 * one and refuses to redefine a version whose graph has changed. Instances already running on that
 * version would otherwise have the graph swapped underneath them mid-flight, one node at a time.
 */
public final class DefinitionRegistry {

    private static final System.Logger LOG = System.getLogger(DefinitionRegistry.class.getName());

    /** Above this many nodes a definition is left to the one-node-at-a-time path: the whole point
     *  of lazy loading is that a big topology is never materialised to advance a single token. */
    private static final int DEF_MAX_NODES = 40;

    private final Storage storage;
    private final ConcurrentHashMap<String, ExecutionMode> modeCache = new ConcurrentHashMap<>();

    /** Small definitions held whole, so the drive loop reads their nodes without touching the
     *  store. Per registry, never static: the key is {@code name:version}, which says nothing about
     *  which database it came from, and two cells in one JVM may serve different graphs under the
     *  same name. Bounded per entry by {@link #DEF_MAX_NODES} and in count by the versions this
     *  cell serves -- the same order as {@link #modeCache}. */
    private final ConcurrentHashMap<String, WorkflowDefinition> defCache = new ConcurrentHashMap<>();

    public DefinitionRegistry(Storage storage) {
        this.storage = storage;
    }

    /** {@link #register(WorkflowDefinition, boolean)} without forcing. */
    public WorkflowDefinition register(WorkflowDefinition def) {
        return register(def, false);
    }

    /**
     * Publishes a definition. An unregistered version is written; a re-registration of the same
     * graph is a no-op. A re-registration whose graph differs is a conflict (409) unless
     * {@code force} is set, which replaces the stored graph -- a development affordance the server
     * only honours when it is configured to (see {@code WIGGLE_ALLOW_GRAPH_REPLACE}); the gRPC
     * layer rejects an unpermitted force before reaching here.
     *
     * <p>A stored fingerprint from a different algorithm is treated as unknown rather than as a
     * mismatch, and is upgraded in place: a change to how the topology is serialised must not read
     * as a change to the graph.
     */
    public WorkflowDefinition register(WorkflowDefinition def, boolean force) {
        if (RunningMode.resolveMode(def.executionMode()) == ExecutionMode.OBSERVED) {
            ObservedRunningMode.requireObservable(def);
        }
        storage.inTxVoid(new Registration(def, force));
        modeCache.put(def.key(), def.executionMode());
        if (def.numberOfNodes() <= DEF_MAX_NODES) defCache.put(def.key(), def);
        return def;
    }

    /** The execution mode for a version, cached; parses the blob at most once per version. */
    public ExecutionMode executionMode(GraphStore graphs, String name, int version) {
        return modeCache.computeIfAbsent(name + ":" + version, k ->
                graphs.definition(name, version)
                        .map(body -> WorkflowDefinition.fromJson(Json.parse(body)).executionMode())
                        .orElse(ExecutionMode.DEFAULT));
    }

    /**
     * A graph handle for the engine. A definition small enough to be held whole is served from
     * memory; anything larger fetches one node at a time and holds no whole graph.
     *
     * <p>Which of the two it is, is decided once per handle rather than once per node: a handle
     * lives for one transaction, so this also keeps a drive pass reading one consistent source
     * rather than switching midway if the definition happens to be cached while it runs.
     */
    public LazyGraph graph(GraphStore graphs, String name, int version) {
        WorkflowDefinition cached = defCache.get(name + ":" + version);
        return cached != null ? new CachedGraph(cached) : new DefaultLazyGraph(graphs, name, version);
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

    private static final class Registration implements Consumer<Tx> {
        private final String fingerprint;
        private final boolean force;
        private final WorkflowDefinition def;

        public Registration(WorkflowDefinition def, boolean force) {
            this.fingerprint = def.fingerprint();
            this.force = force;
            this.def = def;
        }

        @Override
        public void accept(Tx tx) {
            GraphStore.StoredFingerprint stored = tx.definitionFingerprint(def.name(), def.version()).orElse(null);
            boolean comparable = stored != null && stored.value() != null
                    && WorkflowDefinition.FINGERPRINT_ALGO.equals(stored.algo());
            if (comparable && stored.value().equals(fingerprint)) return;   // same graph: nothing to do
            if (comparable && !force) {
                throw EngineException.conflict("workflow '" + def.key() + "' is already registered with a "
                        + "different graph; publish it under a new version");
            }
            if (stored != null) {
                LOG.log(System.Logger.Level.WARNING, () -> "replacing the registered graph of '" + def.key()
                        + "'" + (comparable ? " (forced)" : " (no comparable fingerprint stored)")
                        + "; instances already running on this version will see the new graph");
                tx.deleteGraph(def.name(), def.version());
                tx.replaceDefinition(def.name(), def.version(), Json.write(def.toJson()),
                        fingerprint, WorkflowDefinition.FINGERPRINT_ALGO);
            } else {
                tx.putDefinition(def.name(), def.version(), Json.write(def.toJson()),
                        fingerprint, WorkflowDefinition.FINGERPRINT_ALGO);
            }
            tx.putGraph(def);
        }

    }

    /** A handle over a definition already held whole: every lookup is a map read, no store, no
     *  per-node query. {@link WorkflowDefinition#node} raises the same error for an unknown node
     *  as {@link DefaultLazyGraph} does. */
    private record CachedGraph(WorkflowDefinition def) implements LazyGraph {

        @Override public String name() { return def.name(); }

        @Override public int version() { return def.version(); }

        @Override public String key() { return def.key(); }

        @Override public String startNode() { return def.startNode(); }

        @Override public Node node(String id) { return def.node(id); }

        @Override public Optional<Node> find(String id) { return Optional.ofNullable(def.nodes().get(id)); }
    }
}
