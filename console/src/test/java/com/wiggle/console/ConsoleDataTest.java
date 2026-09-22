package com.wiggle.console;

import com.wiggle.client.CoordinatedConnection;
import com.wiggle.client.DirectConnection;
import com.wiggle.client.WiggleClient;
import com.wiggle.client.WiggleConnection;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.core.AnomalyView;
import com.wiggle.core.ExecutionMode;
import com.wiggle.core.NodeStats;
import com.wiggle.core.WorkflowDefinition;
import com.wiggle.server.engine.WorkflowEngine.StepInput;
import com.wiggle.core.InstanceView;
import com.wiggle.core.Tls;
import com.wiggle.proto.RegisteredNode;
import com.wiggle.proto.RingSlot;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import com.wiggle.server.coord.CoordinatorApi;
import com.wiggle.server.coord.CoordinatorService;
import com.wiggle.server.coord.InMemoryCoordinatorStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The console's gRPC-backed {@link DashboardData}: it lists/details/cancels instances the same way in
 * direct mode (one cluster) and coordinator mode (fanned out across a namespace's cells, operate-by-id
 * routed to the owning cell). Instances are started without a worker, so they sit RUNNING at their
 * first step -- enough to exercise the read/ops surface.
 */
class ConsoleDataTest {

    static {
        System.setProperty("wiggle.observe.settleMillis", "0");   // observed runs are judged at settle; no grace here
    }

    /** The steps a spec names. A worker binds them by name; nothing here implements them. */
    interface Steps {
        Map<String, Object> work(Map<String, Object> ctx);
        Map<String, Object> more(Map<String, Object> ctx);
    }

    private static FlowSpec wf() {
        return FlowSpec.define("wf", 1, Map.class, Steps.class, (f, s) -> f.thenApply(s::work));
    }

    private static ServerConfig config() {
        return new ServerConfig(0, "console-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    @Test @DisplayName("direct mode: lists, details, and cancels instances against one cluster")
    void directMode() throws Exception {
        try (WiggleServer server = new WiggleServer(config()).start();
             DirectConnection conn = WiggleConnection.direct(server.baseUrl())) {
            WiggleClient c = conn.client();
            c.register(wf());
            for (int i = 0; i < 3; i++) c.start("wf", Map.of("i", i), null, null);
            String target = c.start("wf", Map.of(), null, "cust-B");

            GrpcDashboardData data = new GrpcDashboardData(new ConsoleBackend.Direct(conn));

            assertEquals(4, data.listInstances(null, null, 100).size(), "all instances");
            assertEquals(4, data.listInstances("wf", "RUNNING", 100).size(), "filtered by workflow+status");
            assertTrue(data.workflowNames().contains("wf"), "workflow names");

            List<InstanceView> byKey = data.findByCorrelation("cust-B", 100);
            assertEquals(1, byKey.size(), "correlation lookup finds the one match");
            assertEquals(target, byKey.get(0).id(), "correlation returns the right instance");
            assertEquals(0, data.findByCorrelation("cust-none", 100).size(), "unknown correlation -> empty");

            DashboardData.InstanceDetail detail = data.instance(target).orElseThrow();
            assertEquals(target, detail.instance().id());
            assertFalse(detail.tokens().isEmpty(), "a READY token is present");
            assertTrue(data.instance("nope").isEmpty(), "unknown id -> empty");

            data.cancel(target, "from test");
            assertEquals("CANCELLED", data.instance(target).orElseThrow().instance().status(), "cancel routed");
            assertEquals(0, data.pendingSignals(10).size(), "pending signals degrade to empty over gRPC");
        }
    }

    @Test @DisplayName("coordinator mode: fans listing across the namespace's cells; operate-by-id routes")
    void coordinatorMode() throws Exception {
        InMemoryCoordinatorStore store = new InMemoryCoordinatorStore();
        try (WiggleServer cell = new WiggleServer(config().withNamespace("acme")).start();
             CoordinatorService svc = new CoordinatorService(store);
             CoordinatorApi coord = new CoordinatorApi(svc, 0, Tls.Options.DISABLED)) {
            coord.start();
            svc.doRegister("acme", RegisteredNode.newBuilder().setCellId("CellA")
                    .setName("cell-node").setEndpoint(cell.baseUrl()).setRegion("eu-west").build());
            svc.doOpenEpoch("acme", List.of(RingSlot.newBuilder().setShard(0).setCellId("CellA").build()));

            try (CoordinatedConnection conn = WiggleConnection.coordinator("127.0.0.1:" + coord.port(),
                    Tls.Options.DISABLED, "eu-west")) {
                WiggleClient starter = conn.clientForNamespace("acme");
                starter.register(wf());
                String a = starter.start("wf", Map.of());
                String b = starter.start("wf", Map.of());
                String keyed = starter.start("wf", Map.of(), null, "cust-Z");

                GrpcDashboardData data = new GrpcDashboardData(
                        new ConsoleBackend.Coordinated(conn, "acme", Tls.Options.DISABLED));

                List<InstanceView> all = data.listInstances(null, null, 100);
                assertTrue(all.stream().anyMatch(v -> v.id().equals(a))
                        && all.stream().anyMatch(v -> v.id().equals(b)), "fanned out to CellA");

                List<InstanceView> byKey = data.findByCorrelation("cust-Z", 100);
                assertTrue(byKey.stream().anyMatch(v -> v.id().equals(keyed)), "correlation fanned across cells");

                assertEquals(a, data.instance(a).orElseThrow().instance().id(), "detail routed by id");
                data.cancel(a, "from test");
                assertEquals("CANCELLED", data.instance(a).orElseThrow().instance().status(), "cancel routed by id");
            }
        }
    }

    @Test @DisplayName("direct mode: step stats and anomalies read through the seam")
    void statsAndAnomalies() throws Exception {
        try (WiggleServer server = new WiggleServer(config()).start();
             DirectConnection conn = WiggleConnection.direct(server.baseUrl())) {
            FlowSpec spec = FlowSpec.define("obs", 1, Map.class, Steps.class, (f, s) -> f.thenApply(s::work).thenApply(s::more));
            WorkflowDefinition d = spec.definition();
            conn.client().register(new FlowSpec(new WorkflowDefinition(d.name(), d.version(), d.startNode(), d.nodes(),
                    d.queues(), ExecutionMode.OBSERVED, d.checkpoints())));
            String a = d.startNode(), b = d.node(a).next();
            long t0 = 1_700_000_000_000L;
            server.engine().observe("obs", null, null, "r1", "app", List.of(
                    new StepInput(a, null, null, null, t0, t0 + 5),
                    new StepInput(b, null, null, null, t0 + 5, t0 + 25)), true);
            server.engine().observe("obs", null, null, "r2", "app", List.of(
                    new StepInput(a, null, null, null, t0, t0 + 5)), true);   // closed before END
            server.engine().settleObservedRuns(10);

            GrpcDashboardData data = new GrpcDashboardData(new ConsoleBackend.Direct(conn));
            List<NodeStats> stats = data.stepStats("obs", null, 0, 100);
            assertEquals(2, stats.size());
            assertEquals("more", stats.get(0).name(), "slowest p95 first");
            assertEquals(20, stats.get(0).p95Millis());
            assertEquals(2, stats.get(1).count(), "work ran in both runs");
            assertTrue(data.stepStats("obs", null, t0 + 1000, 100).isEmpty(), "the window bounds the sample");

            List<AnomalyView> anomalies = data.anomalies("obs", null, 10);
            assertEquals(1, anomalies.size());
            assertEquals("INCOMPLETE", anomalies.get(0).kind());
            assertEquals(1, data.anomalies(null, anomalies.get(0).instanceId(), 10).size(), "by instance");
            assertTrue(data.anomalies("other", null, 10).isEmpty());
        }
    }
}
