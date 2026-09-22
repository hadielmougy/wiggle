package com.wiggle.tests;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.core.AnomalyView;
import com.wiggle.core.ExecutionMode;
import com.wiggle.core.InstanceView;
import com.wiggle.core.NodeStats;
import com.wiggle.observe.Observed;
import com.wiggle.observe.Observer;
import com.wiggle.observe.ObserverOptions;
import com.wiggle.observe.Run;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import com.wiggle.server.engine.WorkflowEngine;
import com.wiggle.server.store.Rows.Token;
import com.wiggle.server.store.Rows.TokenStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The observe module end to end: an application calls its own step implementation through the
 * wrapped interface, and the server ends up with a timed, conformance-checked instance per run --
 * without a worker, and without the application ever waiting on the server.
 */
class ObserveClientTest {

    static {
        // Runs are judged by the server's settle sweep; keep its grace short so a verdict lands fast.
        System.setProperty("wiggle.observe.settleMillis", "100");
        System.setProperty("wiggle.observe.stallMillis", "600000");
    }

    interface Steps {
        Map<String, Object> a(Map<String, Object> ctx);
        Map<String, Object> b(Map<String, Object> ctx);
        boolean keep(Map<String, Object> ctx);
        Map<String, Object> c(Map<String, Object> ctx);
        /** Not a step of any flow: passes straight through. */
        String helper();
    }

    /** Sleeps a little in b so the timings are visibly non-zero. */
    static final class Impl implements Steps {
        final AtomicInteger calls = new AtomicInteger();
        volatile boolean keep = true;
        volatile RuntimeException failB;

        private static Map<String, Object> put(Map<String, Object> ctx, String k, Object v) {
            Map<String, Object> n = new LinkedHashMap<>(ctx);
            n.put(k, v);
            return n;
        }

        public Map<String, Object> a(Map<String, Object> ctx) { calls.incrementAndGet(); return put(ctx, "a", 1L); }
        public Map<String, Object> b(Map<String, Object> ctx) {
            calls.incrementAndGet();
            if (failB != null) throw failB;
            try { Thread.sleep(20); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            return put(ctx, "b", 2L);
        }
        public boolean keep(Map<String, Object> ctx) { calls.incrementAndGet(); return keep; }
        public Map<String, Object> c(Map<String, Object> ctx) { calls.incrementAndGet(); return put(ctx, "c", 3L); }
        public String helper() { return "plain"; }
    }

    private static FlowSpec spec(String name) {
        return FlowSpec.define(name, 1, Map.class, Steps.class, (f, s) -> f
                .thenApply(s::a)
                .thenApply(s::b)
                .thenFilter(s::keep)
                .thenApply(s::c));
    }

    private static ServerConfig config() {
        return new ServerConfig(TestPorts.free(), "obs-client-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    private static <T> T await(Supplier<T> probe, Duration timeout) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (true) {
            T v = probe.get();
            if (v != null) return v;
            if (System.currentTimeMillis() > deadline) fail("nothing reported within " + timeout);
            Thread.sleep(20);
        }
    }

    private static InstanceView awaitTerminal(WorkflowEngine engine, String workflow, String correlation)
            throws InterruptedException {
        return await(() -> engine.findByCorrelation(correlation, 1).stream()
                .filter(InstanceView::isTerminal).findFirst().orElse(null), Duration.ofSeconds(10));
    }

    @Test @DisplayName("a run of ordinary method calls lands as a COMPLETED instance with per-step timings")
    void runLandsAsTimedInstance() throws Exception {
        FlowSpec spec = spec("obsc-linear");
        Impl impl = new Impl();
        try (WiggleServer server = new WiggleServer(config()).start();
             Observer observer = Observer.connect(server.baseUrl(), ObserverOptions.defaults().withReporter("test-app"))) {
            Observed<Steps> flow = observer.observe(spec, Steps.class, impl);
            Steps s = flow.steps();
            assertEquals("plain", s.helper(), "a method that is no step passes through");

            Run run = flow.begin("order-1");
            Map<String, Object> ctx = s.a(Map.of());
            ctx = s.b(ctx);
            assertTrue(s.keep(ctx));
            ctx = s.c(ctx);
            assertEquals(3L, ctx.get("c"), "the application sees its own results, unchanged");
            assertNull(flow.current(), "reaching END closed the run");
            run.close();
            assertEquals(4, impl.calls.get());

            InstanceView v = awaitTerminal(server.engine(), spec.name(), "order-1");
            assertEquals("COMPLETED", v.status());
            assertEquals(v.id(), run.instanceId());
            assertEquals("RUNNING", run.lastStatus(), "the verdict comes from the settle sweep, after the last report");
            assertEquals(0, observer.dropped());

            Map<String, Token> done = new LinkedHashMap<>();
            for (Token t : server.engine().tokens(v.id())) {
                if (t.status == TokenStatus.DONE && t.startedAt != null) done.put(t.nodeId, t);
            }
            assertEquals(4, done.size(), "every step settled, timed");
            Token b = done.get(spec.definition().node(spec.definition().startNode()).next());
            assertTrue(b.finishedAt - b.startedAt >= 20, "b slept 20ms: " + (b.finishedAt - b.startedAt));
            assertTrue(server.engine().anomalies(spec.name(), null, 10).isEmpty());
            assertEquals(Map.of(), com.wiggle.core.Json.asObject(v.context()), "context is not captured unless asked");
            assertTrue(server.engine().poll("w1", spec.queues(), 10, null).isEmpty(), "nothing was ever dispatched");

            List<NodeStats> stats = server.engine().stepStats(spec.name(), null, 0, 100);
            assertEquals("b", stats.getFirst().name(), "the slept step is the bottleneck");
        }
    }

    @Test @DisplayName("a step called with no run open opens one; the false branch of a filter ends it")
    void implicitRunAndPredicateBranch() throws Exception {
        FlowSpec spec = spec("obsc-implicit");
        Impl impl = new Impl();
        impl.keep = false;
        try (WiggleServer server = new WiggleServer(config()).start();
             Observer observer = Observer.connect(server.baseUrl())) {
            Steps s = observer.observe(spec, Steps.class, impl).steps();
            s.b(s.a(Map.of()));
            assertFalse(s.keep(Map.of()));

            InstanceView v = await(() -> server.engine().list(spec.name(), null, 1).stream()
                    .filter(InstanceView::isTerminal).findFirst().orElse(null), Duration.ofSeconds(10));
            assertTrue(server.engine().anomalies(spec.name(), null, 10).isEmpty(), "a filter's false branch is in the topology");
            assertEquals(2, impl.calls.get() - 1, "c never ran");
        }
    }

    @Test @DisplayName("steps called in the wrong order still report, and the server records the anomaly")
    void outOfOrderIsReported() throws Exception {
        FlowSpec spec = spec("obsc-order");
        Impl impl = new Impl();
        try (WiggleServer server = new WiggleServer(config()).start();
             Observer observer = Observer.connect(server.baseUrl())) {
            Observed<Steps> flow = observer.observe(spec, Steps.class, impl);
            Steps s = flow.steps();
            try (Run run = flow.begin("order-2")) {
                s.b(Map.of());
                s.a(Map.of());
                s.keep(Map.of());
                s.c(Map.of());
            }
            InstanceView v = awaitTerminal(server.engine(), spec.name(), "order-2");
            List<AnomalyView> anomalies = server.engine().anomalies(null, v.id(), 10);
            assertFalse(anomalies.isEmpty());
            assertTrue(anomalies.stream().allMatch(a -> a.kind().equals("OUT_OF_ORDER")), anomalies.toString());
        }
    }

    @Test @DisplayName("a step that throws fails the instance and still throws to the caller")
    void thrownStepFailsInstance() throws Exception {
        FlowSpec spec = spec("obsc-throw");
        Impl impl = new Impl();
        impl.failB = new IllegalStateException("boom");
        try (WiggleServer server = new WiggleServer(config()).start();
             Observer observer = Observer.connect(server.baseUrl())) {
            Observed<Steps> flow = observer.observe(spec, Steps.class, impl);
            Steps s = flow.steps();
            Run run = flow.begin("order-3");
            s.a(Map.of());
            IllegalStateException e = assertThrows(IllegalStateException.class, () -> s.b(Map.of()));
            assertSame(impl.failB, e, "the application's own exception, not a wrapper");
            assertNull(flow.current(), "the throw ended the run");
            run.close();

            InstanceView v = awaitTerminal(server.engine(), spec.name(), "order-3");
            assertEquals("FAILED", v.status());
            assertEquals("b: IllegalStateException: boom", v.error());
        }
    }

    @Test @DisplayName("a run closed before END is reported incomplete")
    void closedEarlyIsIncomplete() throws Exception {
        FlowSpec spec = spec("obsc-early");
        try (WiggleServer server = new WiggleServer(config()).start();
             Observer observer = Observer.connect(server.baseUrl())) {
            Observed<Steps> flow = observer.observe(spec, Steps.class, new Impl());
            try (Run run = flow.begin("order-4")) {
                flow.steps().a(Map.of());
            }
            InstanceView v = awaitTerminal(server.engine(), spec.name(), "order-4");
            assertEquals("FAILED", v.status());
            assertEquals("INCOMPLETE", server.engine().anomalies(null, v.id(), 10).getFirst().kind());
        }
    }

    @Test @DisplayName("a full buffer or an elapsed linger flushes mid-run, so a long run is visible while it runs")
    void midRunVisibility() throws Exception {
        FlowSpec spec = spec("obsc-linger");
        try (WiggleServer server = new WiggleServer(config()).start();
             Observer observer = Observer.connect(server.baseUrl(),
                     ObserverOptions.defaults().withBatchSize(100).withLinger(Duration.ofMillis(50)))) {
            Observed<Steps> flow = observer.observe(spec, Steps.class, new Impl());
            Steps s = flow.steps();
            Run run = flow.begin("order-5");
            s.a(Map.of());
            InstanceView mid = await(() -> server.engine().findByCorrelation("order-5", 1).stream()
                    .findFirst().orElse(null), Duration.ofSeconds(5));
            assertEquals("RUNNING", mid.status(), "one step in, the instance already exists");
            assertEquals(mid.id(), await(run::instanceId, Duration.ofSeconds(5)));
            s.b(Map.of()); s.keep(Map.of()); s.c(Map.of());
            assertEquals("COMPLETED", awaitTerminal(server.engine(), spec.name(), "order-5").status());
        }
    }

    @Test @DisplayName("captureContext ships each step's return as the instance context")
    void capturedContext() throws Exception {
        FlowSpec spec = spec("obsc-ctx");
        try (WiggleServer server = new WiggleServer(config()).start();
             Observer observer = Observer.connect(server.baseUrl(), ObserverOptions.defaults().withCaptureContext(true))) {
            Steps s = observer.observe(spec, Steps.class, new Impl()).steps();
            Map<String, Object> ctx = s.a(Map.of("order", "x"));
            ctx = s.b(ctx); s.keep(ctx); s.c(ctx);
            InstanceView v = await(() -> server.engine().list(spec.name(), "COMPLETED", 1).stream()
                    .findFirst().orElse(null), Duration.ofSeconds(10));
            Map<String, Object> got = com.wiggle.core.Json.asObject(v.context());
            assertEquals("x", got.get("order"));
            assertEquals(3L, got.get("c"));
        }
    }

    @Test @DisplayName("closing the observer flushes what is still buffered")
    void closeFlushes() throws Exception {
        FlowSpec spec = spec("obsc-close");
        try (WiggleServer server = new WiggleServer(config()).start()) {
            Observer observer = Observer.connect(server.baseUrl(),
                    ObserverOptions.defaults().withBatchSize(100).withLinger(Duration.ofMinutes(1)));
            Observed<Steps> flow = observer.observe(spec, Steps.class, new Impl());
            Run run = flow.begin("order-6");
            flow.steps().a(Map.of());
            assertTrue(server.engine().findByCorrelation("order-6", 1).isEmpty(), "still buffered");
            run.close();
            observer.close();
            InstanceView v = server.engine().findByCorrelation("order-6", 1).getFirst();
            assertTrue(server.engine().tokens(v.id()).stream().anyMatch(t -> t.startedAt != null), "the buffered step landed");
            assertEquals("FAILED", awaitTerminal(server.engine(), spec.name(), "order-6").status(),
                    "closed before END: incomplete, but reported");
        }
    }

    @Test @DisplayName("a flow that names a worker mode is refused before anything is published")
    void refusesWorkerRunFlows() throws Exception {
        FlowSpec server = FlowSpec.define("obsc-srv", 1, Map.class, Steps.class, (f, s) -> f
                .executeInServer().thenApply(s::a));
        try (WiggleServer srv = new WiggleServer(config()).start();
             Observer observer = Observer.connect(srv.baseUrl());
             WiggleClient client = new WiggleClient(srv.baseUrl())) {
            assertThrows(IllegalArgumentException.class, () -> observer.observe(server, Steps.class, new Impl()));
            assertFalse(client.workflowNames().contains("obsc-srv"));
        }
    }
}
