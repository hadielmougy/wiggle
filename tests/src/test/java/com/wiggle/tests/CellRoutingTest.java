package com.wiggle.tests;

import com.wiggle.client.CellResolver;
import com.wiggle.client.WiggleClient;
import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.core.IdCodec;
import com.wiggle.core.InstanceView;
import com.wiggle.core.Tls;
import com.wiggle.proto.RegisteredNode;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import com.wiggle.server.coord.CoordinatorApi;
import com.wiggle.server.coord.CoordinatorService;
import com.wiggle.server.coord.InMemoryCoordinatorStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase 2 / T10 (Java SDK): the {@link CellResolver} routes {@code start} and operate-by-id to the
 * cell the coordinator resolves, and falls back to a static target when no coordinator is configured.
 */
class CellRoutingTest {

    private static ServerConfig config() {
        return new ServerConfig(0, "cell-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    private static Blueprint workflow() {
        return Workflow.define("wf").step("a").build();
    }

    @Test @DisplayName("resolver routes start + operate-by-id to the coordinator-resolved cell")
    void routesThroughCoordinator() throws Exception {
        InMemoryCoordinatorStore store = new InMemoryCoordinatorStore();
        try (WiggleServer cell = new WiggleServer(config().withNamespace("acme")).start();
             CoordinatorService svc = new CoordinatorService(store);
             CoordinatorApi coord = new CoordinatorApi(svc, 0, Tls.Options.DISABLED)) {
            coord.start();
            // Simulate the cell's node link registering with the coordinator (seed directly via the service).
            svc.doRegister("acme", RegisteredNode.newBuilder().setCellId("CellA")
                    .setName("cell-node").setEndpoint(cell.baseUrl()).setRegion("eu-west").build());
            svc.doOpenEpoch("acme", java.util.List.of(
                    com.wiggle.proto.RingSlot.newBuilder().setShard(0).setCellId("CellA").build()));

            try (CellResolver resolver = CellResolver.coordinator("127.0.0.1:" + coord.port(),
                    Tls.Options.DISABLED, "eu-west")) {
                WiggleClient starter = resolver.clientForNamespace("acme");
                starter.register(workflow());
                String id = starter.start("wf", Map.of());

                // routed to the namespaced cell -> epoch-aware id
                IdCodec.Placement p = IdCodec.parse(id)
                        .orElseThrow(() -> new AssertionError("expected epoch-aware id, got: " + id));
                assertEquals("acme", p.namespace());

                // operate-by-id resolves to the same cell
                WiggleClient op = resolver.clientForInstance(id);
                InstanceView v = op.instance(id);
                assertNotNull(v);
                assertEquals("RUNNING", v.status());
                assertEquals(1, resolver.activeCellTargets("acme").size(), "one active cell");
            }
        }
    }

    @Test @DisplayName("with no coordinator, the resolver is a pass-through to the static target")
    void directFallback() throws Exception {
        try (WiggleServer cell = new WiggleServer(config()).start();
             CellResolver resolver = CellResolver.direct(cell.baseUrl(), Tls.Options.DISABLED)) {
            WiggleClient client = resolver.clientForNamespace("ignored");
            client.register(workflow());
            String id = client.start("wf", Map.of());
            assertNotNull(id);
            // same client instance is reused for operate-by-id in direct mode
            assertEquals("RUNNING", resolver.clientForInstance(id).instance(id).status());
            assertEquals(1, resolver.activeCellTargets("ignored").size());
        }
    }

    @Test @DisplayName("direct().client() is the zero-namespace entry point for a standalone server")
    void directClientEntryPoint() throws Exception {
        try (WiggleServer cell = new WiggleServer(config()).start();
             CellResolver wiggle = CellResolver.direct(cell.baseUrl())) {   // no-TLS overload
            WiggleClient client = wiggle.client();
            client.register(workflow());
            String id = client.start("wf", Map.of());
            assertNotNull(id);
            assertEquals("RUNNING", client.instance(id).status());
            // the same connection is reused; no namespace label needed
            assertEquals(client, wiggle.client(), "client() returns the one cached client");
        }
    }

    @Test @DisplayName("client() fails under a coordinator, where there is no single cell")
    void clientRejectedUnderCoordinator() throws Exception {
        try (CellResolver resolver = CellResolver.coordinator("127.0.0.1:1", Tls.Options.DISABLED, "eu")) {
            IllegalStateException e = assertThrows(IllegalStateException.class, resolver::client);
            assertTrue(e.getMessage().contains("clientForNamespace"), e.getMessage());
        }
    }
}
