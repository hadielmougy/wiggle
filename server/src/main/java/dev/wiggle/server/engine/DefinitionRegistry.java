package dev.wiggle.server.engine;

import dev.wiggle.core.Json;
import dev.wiggle.core.WorkflowDefinition;
import dev.wiggle.server.store.Storage;
import dev.wiggle.server.store.Tx;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** Read-through cache over the durable definition table. Definitions are immutable per version. */
public final class DefinitionRegistry {

    private final Storage storage;
    private final Map<String, WorkflowDefinition> cache = new ConcurrentHashMap<>();

    public DefinitionRegistry(Storage storage) {
        this.storage = storage;
    }

    public WorkflowDefinition register(WorkflowDefinition def) {
        storage.inTxVoid(tx -> tx.putDefinition(def.name(), def.version(), Json.write(def.toJson())));
        cache.put(def.key(), def);
        return def;
    }

    public WorkflowDefinition get(String name, int version) {
        return lookup(name, version).orElseThrow(
                () -> new IllegalArgumentException("no such workflow definition: " + name + ":" + version));
    }

    public Optional<WorkflowDefinition> lookup(String name, int version) {
        WorkflowDefinition cached = cache.get(name + ":" + version);
        if (cached != null) return Optional.of(cached);
        return storage.inTx(tx -> load(tx, name, version));
    }

    public WorkflowDefinition get(Tx tx, String name, int version) {
        WorkflowDefinition cached = cache.get(name + ":" + version);
        if (cached != null) return cached;
        return load(tx, name, version).orElseThrow(
                () -> new IllegalArgumentException("no such workflow definition: " + name + ":" + version));
    }

    private Optional<WorkflowDefinition> load(Tx tx, String name, int version) {
        return tx.definition(name, version).map(body -> {
            WorkflowDefinition d = WorkflowDefinition.fromJson(Json.parse(body));
            cache.put(d.key(), d);
            return d;
        });
    }

    public Optional<WorkflowDefinition> latest(String name) {
        return storage.inTx(tx -> tx.latestVersion(name).flatMap(v -> load(tx, name, v)));
    }

    public List<String> names() {
        return storage.inTx(Tx::definitionNames);
    }
}
