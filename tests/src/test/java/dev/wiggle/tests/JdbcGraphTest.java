package dev.wiggle.tests;

import dev.wiggle.core.Node;
import dev.wiggle.core.RetryPolicy;
import dev.wiggle.core.WorkflowDefinition;
import dev.wiggle.jdbc.JdbcStorage;
import dev.wiggle.server.store.Storage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Covers the normalised graph tables end to end against a real JDBC engine (H2): a
 * definition is flattened into node/edge rows on register and rebuilt one node at a time,
 * with each edge kind (task next, predicate true/false, fork branches) folded back into
 * the node's typed slots. The in-memory conformance suite never exercises this SQL path.
 */
class JdbcGraphTest {

    private static WorkflowDefinition sampleGraph() {
        RetryPolicy retry = RetryPolicy.exponential(3, Duration.ofMillis(50));
        Map<String, Node> nodes = new LinkedHashMap<>();
        nodes.put("t", Node.task("t", "task", "do-it", "q", retry).withNext("p"));
        nodes.put("p", Node.predicate("p", "check", "is-ok", "q", null).withNext("fk").withAltNext("bad"));
        nodes.put("fk", Node.fork("fk", "split").withBranches(List.of("b1", "b2")));
        nodes.put("b1", Node.task("b1", "left", "left-act", "q", null).withNext("jn"));
        nodes.put("b2", Node.task("b2", "right", "right-act", "q", null).withNext("jn"));
        nodes.put("jn", Node.join("jn", "merge", 2).withNext("ut"));
        // A signal wait with a deadline: next = delivery path, altNext = escalation path.
        nodes.put("ut", Node.signal("ut", "approve", 1000).withNext("df").withAltNext("bad"));
        // A dynamic fork: one branch template, next = the paired (dynamic-width) join.
        nodes.put("df", Node.dynFork("df", "per-item", "items", "item")
                .withBranches(List.of("db")).withNext("djn"));
        nodes.put("db", Node.task("db", "each", "each-act", "q", null).withNext("djn"));
        nodes.put("djn", Node.join("djn", "dyn-merge", 0).withNext("ok"));
        nodes.put("ok", Node.end("ok", true, "done"));
        nodes.put("bad", Node.end("bad", false, "nope"));
        int version = WorkflowDefinition.contentVersion("sample", "t", nodes.values(),
                dev.wiggle.core.ExecutionMode.DEFAULT, java.util.Set.of());
        return new WorkflowDefinition("sample", version, "t", nodes, java.util.Set.of("q"));
    }

    @Test @DisplayName("a definition round-trips through the normalised JDBC graph tables")
    void jdbcGraphRoundTrip() {
        WorkflowDefinition def = sampleGraph();
        try (Storage storage = new JdbcStorage(
                "jdbc:h2:mem:graph-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1", "sa", "", 2)) {
            storage.migrate();
            storage.inTxVoid(tx -> tx.putGraph(def));

            storage.inTxVoid(tx -> {
                assertEquals("t", tx.graphStartNode("sample", def.version()).orElseThrow());

                // Every node comes back byte-for-byte equal to what went in, edges included.
                for (Node original : def.nodes().values()) {
                    Node loaded = tx.graphNode("sample", def.version(), original.id())
                            .orElseThrow(() -> new AssertionError("missing node " + original.id()));
                    assertEquals(original, loaded, "node " + original.id() + " did not round-trip");
                }

                // Spot-check the typed edge reconstruction directly.
                Node p = tx.graphNode("sample", def.version(), "p").orElseThrow();
                assertEquals("fk", p.next());
                assertEquals("bad", p.altNext());
                Node fk = tx.graphNode("sample", def.version(), "fk").orElseThrow();
                assertEquals(List.of("b1", "b2"), fk.branches());
                Node ut = tx.graphNode("sample", def.version(), "ut").orElseThrow();
                assertEquals("df", ut.next(), "signal delivery path");
                assertEquals("bad", ut.altNext(), "signal escalation path");
                assertEquals(1000, ut.sleepMillis(), "signal deadline");
                Node df = tx.graphNode("sample", def.version(), "df").orElseThrow();
                assertEquals(List.of("db"), df.branches(), "dynamic fork branch template");
                assertEquals("djn", df.next(), "dynamic fork's paired join");
                assertEquals("items", df.itemsKey());
                assertEquals("item", df.itemKey());

                assertTrue(tx.graphNode("sample", def.version(), "nope").isEmpty());
            });

            // Re-registering the same content hash is an idempotent no-op.
            assertDoesNotThrow(() -> storage.inTxVoid(tx -> tx.putGraph(def)));
        }
    }
}
