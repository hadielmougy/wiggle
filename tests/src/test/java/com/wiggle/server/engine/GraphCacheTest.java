package com.wiggle.server.engine;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.flow.Wiggle;
import com.wiggle.core.Node;
import com.wiggle.core.WorkflowDefinition;
import com.wiggle.jdbc.JdbcStorage;
import com.wiggle.server.store.GraphStore;
import com.wiggle.server.store.InMemoryStorage;
import com.wiggle.server.store.Storage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The registry's graph cache: loaded once from the normalised rows, shared, bounded -- and the
 *  bulk row loader proven equal to the per-node one on both stores. */
class GraphCacheTest {

    interface Steps {
        Map<String, Object> validate(Map<String, Object> c);
        boolean inStock(Map<String, Object> c);
        Map<String, Object> charge(Map<String, Object> c);
        Map<String, Object> label(Map<String, Object> c);
        Map<String, Object> settle(Map<String, Object> a, Map<String, Object> b);
    }

    private static FlowSpec spec(String name) {
        return FlowSpec.define(name, Map.class, Steps.class, (f, s) -> {
            var validated = f.thenApply(s::validate).thenFilter(s::inStock);
            var payment = validated.thenApply(s::charge);
            var shipping = validated.thenApply(s::label);
            return Wiggle.allOf(payment, shipping).combine(s::settle);
        });
    }

    /** A GraphStore over {@code storage} that counts what the cache reads. */
    static final class CountingGraphs implements GraphStore {
        final Storage storage;
        final AtomicInteger nodeReads = new AtomicInteger();
        final AtomicInteger bulkReads = new AtomicInteger();

        CountingGraphs(Storage storage) { this.storage = storage; }

        @Override public void putDefinition(String name, int version, String json) {
            storage.inTxVoid(tx -> tx.putDefinition(name, version, json));
        }
        @Override public Optional<String> definition(String name, int version) {
            return storage.inTx(tx -> tx.definition(name, version));
        }
        @Override public Optional<Integer> latestVersion(String name) {
            return storage.inTx(tx -> tx.latestVersion(name));
        }
        @Override public List<String> definitionNames() {
            return storage.inTx(GraphStore::definitionNames);
        }
        @Override public void putGraph(WorkflowDefinition def) {
            storage.inTxVoid(tx -> tx.putGraph(def));
        }
        @Override public Optional<Node> graphNode(String workflow, int version, String nodeId) {
            nodeReads.incrementAndGet();
            return storage.inTx(tx -> tx.graphNode(workflow, version, nodeId));
        }
        @Override public List<Node> graphNodes(String workflow, int version) {
            bulkReads.incrementAndGet();
            return storage.inTx(tx -> tx.graphNodes(workflow, version));
        }
        @Override public Optional<String> graphStartNode(String workflow, int version) {
            return storage.inTx(tx -> tx.graphStartNode(workflow, version));
        }
    }

    @Test @DisplayName("one bulk load, then every graph() and node() is memory")
    void warmOnce() {
        InMemoryStorage storage = new InMemoryStorage();
        DefinitionRegistry reg = new DefinitionRegistry(storage, 256);
        WorkflowDefinition def = spec("cache-warm").definition();
        reg.register(def);
        CountingGraphs graphs = new CountingGraphs(storage);

        LazyGraph g = reg.graph(graphs, def.name(), def.version());
        assertEquals(1, graphs.bulkReads.get());

        g.node(g.startNode());
        LazyGraph again = reg.graph(graphs, def.name(), def.version());
        assertSame(g, again, "the cached instance is shared");
        for (Node n : storage.inTx(tx -> tx.graphNodes(def.name(), def.version()))) {
            assertSame(g.node(n.id()), again.node(n.id()));
        }
        assertEquals(1, graphs.bulkReads.get(), "no second bulk load");
        assertEquals(0, graphs.nodeReads.get(), "no per-node reads at all");
    }

    @Test @DisplayName("versions are cached separately -- the key carries the version")
    void versionsAreDistinct() {
        InMemoryStorage storage = new InMemoryStorage();
        DefinitionRegistry reg = new DefinitionRegistry(storage, 256);
        WorkflowDefinition v1 = spec("cache-v").definition();
        // A different topology under the same name hashes to a different version.
        WorkflowDefinition v2 = FlowSpec.define("cache-v", Map.class, Steps.class,
                (f, s) -> f.thenApply(s::validate).thenApply(s::charge)).definition();
        reg.register(v1);
        reg.register(v2);
        assertTrue(v1.version() != v2.version(), "content hash separates them");
        CountingGraphs graphs = new CountingGraphs(storage);

        LazyGraph g1 = reg.graph(graphs, "cache-v", v1.version());
        LazyGraph g2 = reg.graph(graphs, "cache-v", v2.version());
        assertNotSame(g1, g2);
        assertEquals(v1.version(), g1.version());
        assertEquals(v2.version(), g2.version());
        assertThrows(IllegalStateException.class, () -> g2.node(missingIn(storage, v2, v1)),
                "v1-only nodes are unknown to v2's cached graph");
    }

    private static String missingIn(Storage storage, WorkflowDefinition inThis, WorkflowDefinition fromThat) {
        List<Node> have = storage.inTx(tx -> tx.graphNodes(inThis.name(), inThis.version()));
        List<Node> other = storage.inTx(tx -> tx.graphNodes(fromThat.name(), fromThat.version()));
        return other.stream().map(Node::id)
                .filter(id -> have.stream().noneMatch(n -> n.id().equals(id)))
                .findFirst().orElseThrow();
    }

    @Test @DisplayName("cap 0 disables the cache: the lazy per-node path serves everything")
    void capZeroDisables() {
        InMemoryStorage storage = new InMemoryStorage();
        DefinitionRegistry reg = new DefinitionRegistry(storage, 0);
        WorkflowDefinition def = spec("cache-off").definition();
        reg.register(def);
        CountingGraphs graphs = new CountingGraphs(storage);

        LazyGraph g = reg.graph(graphs, def.name(), def.version());
        g.node(g.startNode());
        assertEquals(0, graphs.bulkReads.get());
        assertTrue(graphs.nodeReads.get() > 0, "served node-at-a-time, as before the cache");
    }

    @Test @DisplayName("past the cap, a graph is served lazily instead of evicting")
    void capOverflowFallsBackToLazy() {
        InMemoryStorage storage = new InMemoryStorage();
        DefinitionRegistry reg = new DefinitionRegistry(storage, 1);
        WorkflowDefinition a = spec("cache-a").definition();
        WorkflowDefinition b = spec("cache-b").definition();
        reg.register(a);
        reg.register(b);
        CountingGraphs graphs = new CountingGraphs(storage);

        reg.graph(graphs, a.name(), a.version());
        assertEquals(1, graphs.bulkReads.get());
        LazyGraph gb = reg.graph(graphs, b.name(), b.version());
        gb.node(gb.startNode());
        assertEquals(1, graphs.bulkReads.get(), "no bulk load past the cap");
        assertTrue(graphs.nodeReads.get() > 0, "the overflow graph reads node-at-a-time");
    }

    @Test @DisplayName("an unknown workflow keeps today's error shape")
    void unknownWorkflowParity() {
        DefinitionRegistry reg = new DefinitionRegistry(new InMemoryStorage(), 256);
        CountingGraphs graphs = new CountingGraphs(new InMemoryStorage());
        LazyGraph g = reg.graph(graphs, "nope", 1);
        assertThrows(IllegalArgumentException.class, g::startNode);
    }

    @Test @DisplayName("bulk load equals per-node load, node for node, on both stores")
    void bulkMatchesSingleOnBothStores() throws Exception {
        WorkflowDefinition def = spec("cache-parity").definition();

        InMemoryStorage mem = new InMemoryStorage();
        mem.inTxVoid(tx -> tx.putGraph(def));
        assertParity(mem, def);

        try (JdbcStorage h2 = new JdbcStorage(
                "jdbc:h2:mem:graph-parity-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
                "sa", "", 2, new com.wiggle.postgres.H2Dialect())) {
            h2.migrate();
            h2.inTxVoid(tx -> tx.putGraph(def));
            assertParity(h2, def);
        }
    }

    private static void assertParity(Storage storage, WorkflowDefinition def) {
        List<Node> bulk = storage.inTx(tx -> tx.graphNodes(def.name(), def.version()));
        assertFalse(bulk.isEmpty());
        for (Node fromBulk : bulk) {
            Node fromSingle = storage.inTx(tx ->
                    tx.graphNode(def.name(), def.version(), fromBulk.id())).orElseThrow();
            assertEquals(fromSingle, fromBulk,
                    "bulk and per-node assembly disagree at '" + fromBulk.id() + "'");
        }
    }
}
