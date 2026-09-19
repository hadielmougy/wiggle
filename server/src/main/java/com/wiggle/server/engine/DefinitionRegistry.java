package com.wiggle.server.engine;

import com.wiggle.core.ExecutionMode;
import com.wiggle.core.Json;
import com.wiggle.core.WorkflowDefinition;
import com.wiggle.server.store.GraphStore;
import com.wiggle.server.store.Storage;
import com.wiggle.server.store.Tx;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

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

    private final Storage storage;
    // A definition's execution mode is a single immutable enum per version -- cheap to cache,
    // unlike the whole graph. Lets the hot poll path stamp the mode without parsing the blob.
    private final ConcurrentHashMap<String, ExecutionMode> modeCache = new ConcurrentHashMap<>();

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
        String fingerprint = def.fingerprint();
        storage.inTxVoid(tx -> {
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

    /** A memory-thrifty handle for the engine: fetches one node at a time, holds no whole graph. */
    public LazyGraph graph(GraphStore graphs, String name, int version) {
        return new LazyGraph(graphs, name, version);
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
