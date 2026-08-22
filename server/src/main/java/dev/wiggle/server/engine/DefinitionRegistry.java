package dev.wiggle.server.engine;

import dev.wiggle.core.ExecutionMode;
import dev.wiggle.core.Json;
import dev.wiggle.core.WorkflowDefinition;
import dev.wiggle.server.store.Storage;
import dev.wiggle.server.store.Tx;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Owns workflow definitions. Registration stores two things: the raw submitted graph as a
 * write-once blob (source of truth for audit/replay) and a normalised set of node/edge rows
 * the engine reads one node at a time. The hot path never materialises a whole graph, so no
 * full-graph cache lives here -- that in-memory copy was exactly the pressure we shed. The
 * blob-loading {@code latest}/{@code lookup} paths remain for admin/describe calls only.
 */
public final class DefinitionRegistry {

    private final Storage storage;
    // A definition's execution mode is a single immutable enum per version -- cheap to cache,
    // unlike the whole graph. Lets the hot poll path stamp the mode without parsing the blob.
    private final ConcurrentHashMap<String, ExecutionMode> modeCache = new ConcurrentHashMap<>();

    public DefinitionRegistry(Storage storage) {
        this.storage = storage;
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
    public ExecutionMode executionMode(Tx tx, String name, int version) {
        return modeCache.computeIfAbsent(name + ":" + version, k ->
                tx.definition(name, version)
                        .map(body -> WorkflowDefinition.fromJson(Json.parse(body)).executionMode())
                        .orElse(ExecutionMode.DEFAULT));
    }

    /** A memory-thrifty handle for the engine: fetches one node at a time, holds no whole graph. */
    public LazyGraph graph(Tx tx, String name, int version) {
        return new LazyGraph(tx, name, version);
    }

    public WorkflowDefinition get(String name, int version) {
        return lookup(name, version).orElseThrow(
                () -> new IllegalArgumentException("no such workflow definition: " + name + ":" + version));
    }

    public Optional<WorkflowDefinition> lookup(String name, int version) {
        return storage.inTx(tx -> load(tx, name, version));
    }

    private Optional<WorkflowDefinition> load(Tx tx, String name, int version) {
        return tx.definition(name, version).map(body -> WorkflowDefinition.fromJson(Json.parse(body)));
    }

    public Optional<WorkflowDefinition> latest(String name) {
        return storage.inTx(tx -> tx.latestVersion(name).flatMap(v -> load(tx, name, v)));
    }

    public List<String> names() {
        return storage.inTx(Tx::definitionNames);
    }
}
