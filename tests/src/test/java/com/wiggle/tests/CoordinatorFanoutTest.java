package com.wiggle.tests;

import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.core.Json;
import com.wiggle.proto.RegisterWorkflowResponse;
import com.wiggle.proto.RegisteredNode;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import com.wiggle.server.coord.CoordinatorService;
import com.wiggle.server.coord.InMemoryCoordinatorStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
        Blueprint bp = Workflow.define("wf").step("a").build();
        return Json.write(bp.definition().toJson()).getBytes(StandardCharsets.UTF_8);
    }

    private static void register(CoordinatorService coord, WiggleServer cell) {
        coord.doRegister("orders", RegisteredNode.newBuilder().setCellId("CellA")
                .setName(cell.baseUrl()).setEndpoint(cell.baseUrl()).build());
    }

    @Test @DisplayName("RegisterWorkflow fans out to every cell; a joining cell is seeded from a sibling")
    void fanOutAndSeedOnJoin() throws Exception {
        InMemoryCoordinatorStore store = new InMemoryCoordinatorStore();
        try (WiggleServer a = new WiggleServer(cell()).start();
             WiggleServer b = new WiggleServer(cell()).start();
             CoordinatorService coord = new CoordinatorService(store)) {

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

    @Test @DisplayName("re-registering an unchanged definition is a no-op (no re-fan-out)")
    void reRegisterUnchangedIsNoOp() throws Exception {
        InMemoryCoordinatorStore store = new InMemoryCoordinatorStore();
        try (WiggleServer a = new WiggleServer(cell()).start();
             WiggleServer b = new WiggleServer(cell()).start();
             CoordinatorService coord = new CoordinatorService(store)) {
            register(coord, a);
            register(coord, b);

            RegisterWorkflowResponse first = coord.doRegisterWorkflow("orders", "wf", definitionJson());
            assertEquals(2, first.getCellsSeeded(), "first register fans out to both cells");

            RegisterWorkflowResponse again = coord.doRegisterWorkflow("orders", "wf", definitionJson());
            assertEquals(0, again.getCellsSeeded(), "unchanged definition -> fan-out skipped");
            assertEquals(first.getVersion(), again.getVersion(), "same content-hash version returned");
        }
    }

    @Test @DisplayName("Dump reflects the placement policy, node roster and definition registry")
    void dumpReflectsState() throws Exception {
        InMemoryCoordinatorStore store = new InMemoryCoordinatorStore();
        try (WiggleServer a = new WiggleServer(cell()).start();
             CoordinatorService coord = new CoordinatorService(store)) {
            register(coord, a);
            coord.doOpenEpoch("orders", java.util.List.of(
                    com.wiggle.proto.RingSlot.newBuilder().setShard(0).setCellId("CellA").build()));
            coord.doRegisterWorkflow("orders", "wf", definitionJson());

            String json = coord.doDump().getJson();
            assertTrue(json.contains("\"policies\"") && json.contains("\"orders\""), json);
            assertTrue(json.contains("\"nodes\"") && json.contains("CellA"), json);
            assertTrue(json.contains("\"definitions\"") && json.contains("\"wf\""), json);
        }
    }

    @Test @DisplayName("allocate then deallocate: list reflects it, and deregister is idempotent")
    void allocateListDeallocate() throws Exception {
        InMemoryCoordinatorStore store = new InMemoryCoordinatorStore();
        try (WiggleServer a = new WiggleServer(cell()).start();
             CoordinatorService coord = new CoordinatorService(store)) {
            coord.doRegister("orders", RegisteredNode.newBuilder().setCellId("CellA")
                    .setName(a.baseUrl()).setEndpoint(a.baseUrl()).build());

            coord.doRegisterWorkflow("orders", "wf", definitionJson());
            assertEquals(1, coord.doListWorkflows("orders").getWorkflowsCount(), "allocated -> listed");
            assertEquals("wf", coord.doListWorkflows("orders").getWorkflows(0).getName());

            assertTrue(coord.doDeregisterWorkflow("orders", "wf").getRemoved(), "deallocate removes it");
            assertEquals(0, coord.doListWorkflows("orders").getWorkflowsCount(), "gone from the registry");
            assertFalse(coord.doDeregisterWorkflow("orders", "wf").getRemoved(), "second deallocate is a no-op");
        }
    }

    @Test @DisplayName("RegisterWorkflow with no cell for the namespace fails")
    void noCell() throws Exception {
        InMemoryCoordinatorStore store = new InMemoryCoordinatorStore();
        try (CoordinatorService coord = new CoordinatorService(store)) {
            try {
                coord.doRegisterWorkflow("orders", "wf", definitionJson());
                throw new AssertionError("expected failure with no cell");
            } catch (IllegalStateException expected) {
                assertTrue(expected.getMessage().contains("no cell"));
            }
        }
    }
}
