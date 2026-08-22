package dev.wiggle.server.engine;

import dev.wiggle.core.ExecutionMode;
import dev.wiggle.core.Node;
import dev.wiggle.core.WorkflowDefinition;
import dev.wiggle.server.store.InMemoryStorage;
import dev.wiggle.server.store.Storage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * White-box test (same package) for the memory-thrifty graph handle: correctness must be
 * unaffected by its bounded per-transaction LRU, including once a graph is larger than the
 * cache and early nodes have been evicted.
 */
class LazyGraphTest {

    /** Larger than LazyGraph's 64-entry LRU, so a full traversal forces evictions. */
    private static final int NODES = 100;

    private static WorkflowDefinition longChain() {
        Map<String, Node> nodes = new LinkedHashMap<>();
        for (int i = 0; i < NODES; i++) {
            String next = i == NODES - 1 ? "end" : "n" + (i + 1);
            nodes.put("n" + i, Node.task("n" + i, "step-" + i, "act-" + i, "q", null).withNext(next));
        }
        nodes.put("end", Node.end("end", true, null));
        int version = WorkflowDefinition.contentVersion("lazy", "n0", nodes.values(),
                ExecutionMode.DEFAULT, Set.of());
        return new WorkflowDefinition("lazy", version, "n0", nodes, Set.of("q"));
    }

    @Test @DisplayName("a graph larger than the LRU stays correct through eviction and re-access")
    void evictionIsTransparent() {
        WorkflowDefinition def = longChain();
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            new DefinitionRegistry(storage).register(def);
            storage.inTxVoid(tx -> {
                LazyGraph graph = new LazyGraph(tx, def.name(), def.version());
                assertEquals("n0", graph.startNode());
                // Walk the whole chain: past node 64 the earliest entries get evicted.
                for (int i = 0; i < NODES; i++) {
                    Node n = graph.node("n" + i);
                    assertEquals("act-" + i, n.activity(), "node n" + i + " resolves correctly");
                }
                // Re-fetch an evicted early node: reloaded from storage, still correct.
                assertEquals("n1", graph.node("n0").next(), "evicted node reloads correctly");
                assertEquals("lazy:" + def.version(), graph.key());
            });
        }
    }

    @Test @DisplayName("an unknown node and a missing definition fail with clear errors")
    void failsClearly() {
        WorkflowDefinition def = longChain();
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            new DefinitionRegistry(storage).register(def);
            storage.inTxVoid(tx -> {
                LazyGraph graph = new LazyGraph(tx, def.name(), def.version());
                assertThrows(IllegalStateException.class, () -> graph.node("nope"));

                LazyGraph missing = new LazyGraph(tx, "no-such-workflow", 1);
                assertThrows(IllegalArgumentException.class, missing::startNode);
            });
        }
    }
}
