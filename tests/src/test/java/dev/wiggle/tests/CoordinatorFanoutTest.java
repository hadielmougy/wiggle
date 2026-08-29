package dev.wiggle.tests;

import dev.wiggle.client.dsl.Blueprint;
import dev.wiggle.client.dsl.Workflow;
import dev.wiggle.core.Json;
import dev.wiggle.core.Tls;
import dev.wiggle.proto.RegisterWorkflowResponse;
import dev.wiggle.proto.RegisteredNode;
import dev.wiggle.server.ServerConfig;
import dev.wiggle.server.WiggleServer;
import dev.wiggle.server.coord.CoordinatorApi;
import dev.wiggle.server.coord.InMemoryCoordinatorStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase 3 / T11: RegisterWorkflow fans a definition out to every cell of a namespace, and a joining
 * cell is seeded from a sibling before it enters the roster (R23).
 */
class CoordinatorFanoutTest {

    private static ServerConfig cell() {
        return new ServerConfig(0, "cell", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10)).withNamespace("orders");
    }

    private static byte[] definitionJson() {
        Blueprint<Map<String, Object>> bp = Workflow.define("wf").step("a", c -> c).build();
        return Json.write(bp.definition().toJson()).getBytes(StandardCharsets.UTF_8);
    }

    private static void register(CoordinatorApi coord, WiggleServer cell) {
        coord.doRegister("orders", RegisteredNode.newBuilder()
                .setName(cell.baseUrl()).setEndpoint(cell.baseUrl()).build());
    }

    @Test @DisplayName("RegisterWorkflow fans out to every cell; a joining cell is seeded from a sibling")
    void fanOutAndSeedOnJoin() throws Exception {
        InMemoryCoordinatorStore store = new InMemoryCoordinatorStore();
        try (WiggleServer a = new WiggleServer(cell()).start();
             WiggleServer b = new WiggleServer(cell()).start();
             CoordinatorApi coord = new CoordinatorApi(store, 0, Tls.Options.DISABLED)) {

            register(coord, a);
            register(coord, b);

            RegisterWorkflowResponse r = coord.doRegisterWorkflow("orders", "wf", definitionJson());
            assertEquals(2, r.getCellsSeeded(), "fanned out to both cells");
            assertTrue(r.getVersion() > 0);

            // both cells now hold the workflow, at the same content-hash version
            assertEquals(r.getVersion(), a.engine().latestDefinition("wf").orElseThrow().version());
            assertEquals(r.getVersion(), b.engine().latestDefinition("wf").orElseThrow().version());

            // a third cell joins AFTER the definition exists -> seeded from a sibling before it's eligible
            try (WiggleServer c = new WiggleServer(cell()).start()) {
                register(coord, c);
                assertEquals(r.getVersion(), c.engine().latestDefinition("wf")
                        .orElseThrow(() -> new AssertionError("joining cell was not seeded")).version());
            }
        }
    }

    @Test @DisplayName("RegisterWorkflow with no cell for the namespace fails")
    void noCell() throws Exception {
        InMemoryCoordinatorStore store = new InMemoryCoordinatorStore();
        try (CoordinatorApi coord = new CoordinatorApi(store, 0, Tls.Options.DISABLED)) {
            try {
                coord.doRegisterWorkflow("orders", "wf", definitionJson());
                throw new AssertionError("expected failure with no cell");
            } catch (IllegalStateException expected) {
                assertTrue(expected.getMessage().contains("no cell"));
            }
        }
    }
}
