package com.wiggle.console;

import com.wiggle.client.DirectConnection;
import com.wiggle.client.WiggleClient;
import com.wiggle.client.WiggleConnection;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.ForFlow;
import com.wiggle.client.worker.Worker;
import com.wiggle.core.NodeStats;
import com.wiggle.core.InstanceView;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The console's gRPC-backed {@link DashboardData}: it lists, details and cancels instances of one
 * cluster. Instances are started without a worker, so they sit RUNNING at their first step -- enough
 * to exercise the read/ops surface.
 */
class ConsoleDataTest {

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

            GrpcDashboardData data = new GrpcDashboardData(conn.client());

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

    /** Runs {@code n} instances of a two-step flow to completion, the second step the slow one. */
    static FlowSpec runTimed(WiggleClient client, int n) throws Exception {
        FlowSpec spec = FlowSpec.define("timed", 1, Map.class, Steps.class, (f, s) -> f.thenApply(s::work).thenApply(s::more));
        client.register(spec);
        try (Worker w = new Worker(client, "timed-w").registerHandler(new TimedSteps()).start()) {
            for (int i = 0; i < n; i++) client.awaitCompletion(client.start(spec, Map.of()), Duration.ofSeconds(20));
        }
        return spec;
    }

    @ForFlow("timed")
    public static final class TimedSteps {
        public Map<String, Object> work(Map<String, Object> ctx) { return ctx; }
        public Map<String, Object> more(Map<String, Object> ctx) throws InterruptedException {
            Thread.sleep(60);
            return ctx;
        }
    }

    @Test @DisplayName("direct mode: step stats read through the seam, slowest first")
    void stepStats() throws Exception {
        try (WiggleServer server = new WiggleServer(config()).start();
             DirectConnection conn = WiggleConnection.direct(server.baseUrl())) {
            long before = System.currentTimeMillis();
            runTimed(conn.client(), 2);

            GrpcDashboardData data = new GrpcDashboardData(conn.client());
            List<NodeStats> stats = data.stepStats("timed", null, 0, 100);
            assertEquals(2, stats.size());
            assertEquals("more", stats.get(0).name(), "slowest p95 first");
            assertTrue(stats.get(0).p95Millis() >= 60, "more sleeps 60 ms: " + stats.get(0));
            assertEquals(2, stats.get(1).count(), "work ran in both runs");
            assertTrue(data.stepStats("timed", null, System.currentTimeMillis() + 60_000, 100).isEmpty(),
                    "the window bounds the sample");
            assertTrue(before > 0);
        }
    }
}
