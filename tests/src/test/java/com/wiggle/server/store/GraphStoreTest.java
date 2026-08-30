package com.wiggle.server.store;

import com.wiggle.core.ExecutionMode;
import com.wiggle.core.Json;
import com.wiggle.core.Node;
import com.wiggle.core.NodeKind;
import com.wiggle.core.WorkflowDefinition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Contract test for the {@link GraphStore} half of the store -- the immutable definition/graph
 * reference data -- exercised through {@link InMemoryStorage}. It documents the interface's
 * behaviour independently of the engine ({@link com.wiggle.server.engine.LazyGraph} tests the
 * lazy view; {@code JdbcGraphTest} the JDBC row normalisation).
 */
class GraphStoreTest {

    /**
     * A small graph: {@code start (task) -> gate (predicate) -> done (task) -> end}, with the
     * gate's false edge going straight to the end. {@code tail} lets a caller extend it so two
     * versions of the same name hash differently.
     */
    private static WorkflowDefinition def(String name, boolean withTail) {
        Map<String, Node> nodes = new LinkedHashMap<>();
        nodes.put("n1", Node.task("n1", "start", name + "#start", "q", null).withNext("n2"));
        nodes.put("n2", Node.predicate("n2", "gate", name + "#gate", "q", null).withNext("n3").withAltNext("end"));
        nodes.put("n3", Node.task("n3", "done", name + "#done", "q", null).withNext(withTail ? "n4" : "end"));
        if (withTail) {
            nodes.put("n4", Node.task("n4", "tail", name + "#tail", "q", null).withNext("end"));
        }
        nodes.put("end", Node.end("end", true, null));
        int version = WorkflowDefinition.contentVersion(name, "n1", nodes.values(), ExecutionMode.DEFAULT, Set.of());
        return new WorkflowDefinition(name, version, "n1", nodes, Set.of("q"));
    }

    /** Registers a definition the way the engine does: the blob plus the normalised graph rows. */
    private static void register(Storage storage, WorkflowDefinition d) {
        storage.inTxVoid(tx -> {
            tx.putDefinition(d.name(), d.version(), Json.write(d.toJson()));
            tx.putGraph(d);
        });
    }

    /** Runs {@code body} against the store's {@link GraphStore} facet in one transaction. */
    private static void read(Storage storage, Consumer<GraphStore> body) {
        storage.inTxVoid(tx -> body.accept(tx));   // Tx is-a GraphStore
    }

    @Test
    @DisplayName("the definition blob round-trips; an unknown one is absent")
    void definitionBlobRoundTrips() {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            WorkflowDefinition d = def("orders", false);
            register(storage, d);
            read(storage, g -> {
                assertTrue(g.definition("orders", d.version()).isPresent());
                assertEquals(WorkflowDefinition.fromJson(Json.parse(g.definition("orders", d.version()).get())).name(), "orders");
                assertTrue(g.definition("orders", 999).isEmpty(), "unknown version");
                assertTrue(g.definition("nope", d.version()).isEmpty(), "unknown name");
            });
        }
    }

    @Test
    @DisplayName("latestVersion is the most recently registered version, and empty for an unknown name")
    void latestVersion() {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            WorkflowDefinition v1 = def("orders", false);
            WorkflowDefinition v2 = def("orders", true);   // different topology -> different hash
            assertFalse(v1.version() == v2.version(), "the two versions must differ");

            register(storage, v1);
            read(storage, g -> assertEquals(v1.version(), g.latestVersion("orders").orElseThrow()));

            register(storage, v2);
            read(storage, g -> assertEquals(v2.version(), g.latestVersion("orders").orElseThrow(),
                    "latest tracks the most recently registered"));

            read(storage, g -> assertTrue(g.latestVersion("unknown").isEmpty()));
        }
    }

    @Test
    @DisplayName("definitionNames lists each registered name once, sorted")
    void definitionNames() {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            register(storage, def("beta", false));
            register(storage, def("alpha", false));
            register(storage, def("alpha", true));   // second version of alpha -> still one name
            read(storage, g -> assertEquals(List.of("alpha", "beta"), g.definitionNames()));
        }
    }

    @Test
    @DisplayName("putGraph normalises the graph so a single node and its edges can be read back")
    void graphNodeReconstructsEdges() {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            WorkflowDefinition d = def("orders", false);
            register(storage, d);
            read(storage, g -> {
                assertEquals("n1", g.graphStartNode("orders", d.version()).orElseThrow());

                Node task = g.graphNode("orders", d.version(), "n1").orElseThrow();
                assertEquals(NodeKind.TASK, task.kind());
                assertEquals("n2", task.next());

                Node gate = g.graphNode("orders", d.version(), "n2").orElseThrow();
                assertEquals(NodeKind.PREDICATE, gate.kind());
                assertEquals("n3", gate.next(), "true edge");
                assertEquals("end", gate.altNext(), "false edge survives the round-trip");
            });
        }
    }

    @Test
    @DisplayName("reads for an unknown node, version, or workflow are absent")
    void unknownReadsAreEmpty() {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            WorkflowDefinition d = def("orders", false);
            register(storage, d);
            read(storage, g -> {
                assertTrue(g.graphNode("orders", d.version(), "ghost").isEmpty(), "unknown node");
                assertTrue(g.graphNode("orders", 999, "n1").isEmpty(), "unknown version");
                assertTrue(g.graphNode("nope", d.version(), "n1").isEmpty(), "unknown workflow");
                assertTrue(g.graphStartNode("nope", 1).isEmpty());
            });
        }
    }

    @Test
    @DisplayName("re-registering the same version is an idempotent no-op")
    void reRegisterIsIdempotent() {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            WorkflowDefinition d = def("orders", false);
            register(storage, d);
            register(storage, d);   // content hash is identical -> must not throw or corrupt
            read(storage, g -> {
                assertEquals("n1", g.graphStartNode("orders", d.version()).orElseThrow());
                assertEquals(List.of("orders"), g.definitionNames());
                assertEquals(NodeKind.PREDICATE, g.graphNode("orders", d.version(), "n2").orElseThrow().kind());
            });
        }
    }
}
