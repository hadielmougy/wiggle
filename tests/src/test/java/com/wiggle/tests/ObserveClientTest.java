package com.wiggle.tests;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.core.AnomalyView;
import com.wiggle.core.ExecutionMode;
import com.wiggle.core.InstanceView;
import com.wiggle.core.NodeStats;
import com.wiggle.client.worker.Compensable;
import com.wiggle.client.worker.CompensableActivity;
import com.wiggle.client.worker.Compensation;
import com.wiggle.observe.Compensations;
import com.wiggle.observe.Observation;
import com.wiggle.observe.ObservedFlow;
import com.wiggle.observe.StepTimer;
import com.wiggle.observe.Observed;
import com.wiggle.observe.Observer;
import com.wiggle.observe.ObserverOptions;
import com.wiggle.observe.Run;
import com.wiggle.observe.RunContext;
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
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
        /** What the step saw as its run's key, through the ambient accessor. */
        volatile String keyInA;

        private static Map<String, Object> put(Map<String, Object> ctx, String k, Object v) {
            Map<String, Object> n = new LinkedHashMap<>(ctx);
            n.put(k, v);
            return n;
        }

        public Map<String, Object> a(Map<String, Object> ctx) {
            calls.incrementAndGet();
            keyInA = Observation.correlationId();
            return put(ctx, "a", 1L);
        }
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
            assertSame(run, flow.current(), "a begun run stays open past END until it is closed");
            assertEquals("order-1", impl.keyInA, "a step reads its run's key ambiently");
            run.close();
            assertNull(flow.current());
            assertThrows(IllegalStateException.class, Observation::run, "nothing is open on this thread now");
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

    @Test @DisplayName("two services join one run by key and the server completes one instance with both reporters")
    void twoServicesJoinOneRun() throws Exception {
        FlowSpec spec = spec("obsc-join");
        Impl impl = new Impl();
        try (WiggleServer server = new WiggleServer(config()).start();
             Observer gateway = Observer.connect(server.baseUrl(), ObserverOptions.defaults().withReporter("gateway"));
             Observer payments = Observer.connect(server.baseUrl(), ObserverOptions.defaults().withReporter("payments"))) {
            Observed<Steps> front = gateway.observe(spec, Steps.class, impl);
            Observed<Steps> back = payments.observe(spec, Steps.class, impl);

            // the originator runs a and b, then hands the run's context on (a header, in real life)
            RunContext ctx;
            try (Run run = front.begin("order-7")) {
                front.steps().b(front.steps().a(Map.of()));
                ctx = Observation.context();
            }
            assertEquals(spec.name(), ctx.workflow());
            assertEquals("order-7", ctx.correlationId());
            String bNode = spec.definition().node(spec.definition().startNode()).next();
            assertEquals(bNode, ctx.after(), "the context names the step completed last before the hand-off");
            assertEquals(4, ctx.toHeaders().size());
            RunContext received = RunContext.fromHeaders(ctx.toHeaders());
            assertEquals(ctx, received, "the context survives a header round trip");
            assertNull(RunContext.fromHeaders(Map.of()), "no run header, no context");

            // the participant joins by that context and runs the rest
            try (Run run = back.join(received)) {
                back.steps().keep(Map.of());
                back.steps().c(Map.of());
                assertEquals("order-7", run.correlationId());
            }

            InstanceView v = awaitTerminal(server.engine(), spec.name(), "order-7");
            assertEquals("COMPLETED", v.status(), "the originator's early close did not fail the run: END was seen");
            assertEquals(1, server.engine().findByCorrelation("order-7", 10).size(), "one instance for the key");
            assertEquals(Set.of("gateway", "payments"), server.engine().tokens(v.id()).stream()
                    .map(t -> t.leaseOwner).filter(o -> o != null).collect(java.util.stream.Collectors.toSet()));
            assertTrue(server.engine().anomalies(null, v.id(), 10).isEmpty(), server.engine().anomalies(null, v.id(), 10).toString());
            String keepNode = spec.definition().node(bNode).next();
            assertEquals(bNode, server.engine().tokens(v.id()).stream().filter(t -> t.nodeId.equals(keepNode))
                    .findFirst().orElseThrow().afterNode, "the participant's first step names the sender's last as its cause");

            FlowSpec other = spec("obsc-other");
            Observed<Steps> elsewhere = gateway.observe(other, Steps.class, impl);
            assertThrows(IllegalArgumentException.class, () -> elsewhere.join(received), "a context names its flow");
        }
    }

    @Test @DisplayName("a run carried to another thread with wrap reports as one run")
    void runCarriedAcrossThreads() throws Exception {
        FlowSpec spec = spec("obsc-thread");
        Impl impl = new Impl();
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try (WiggleServer server = new WiggleServer(config()).start();
             Observer observer = Observer.connect(server.baseUrl())) {
            Observed<Steps> flow = observer.observe(spec, Steps.class, impl);
            Steps s = flow.steps();
            try (Run run = flow.begin("order-8")) {
                s.a(Map.of());
                pool.submit(run.wrap(() -> { s.b(Map.of()); s.keep(Map.of()); })).get();
                assertEquals("order-8", pool.submit(run.wrap(() -> Observation.correlationId())).get(),
                        "the wrapped task sees the run ambiently");
                assertThrows(java.util.concurrent.ExecutionException.class,
                        () -> pool.submit(() -> Observation.run()).get(), "an unwrapped task sees no run");
                s.c(Map.of());
            }
            InstanceView v = awaitTerminal(server.engine(), spec.name(), "order-8");
            assertEquals("COMPLETED", v.status());
            assertTrue(server.engine().anomalies(null, v.id(), 10).isEmpty(), "one run, in order, across two threads");
        } finally {
            pool.shutdownNow();
        }
    }

    /** A saga contract: two steps with an undo, one without. */
    interface BookingSteps {
        CompensableActivity<Map<String, Object>, Map<String, Object>> reserve();
        CompensableActivity<Map<String, Object>, Map<String, Object>> charge();
        Map<String, Object> ship(Map<String, Object> b);
    }

    static final class Booking implements BookingSteps {
        final List<String> undone = new java.util.concurrent.CopyOnWriteArrayList<>();
        volatile boolean chargeFails;

        CompensableActivity<Map<String, Object>, Map<String, Object>> activity(String name, boolean fail) {
            return new CompensableActivity<>() {
                public Map<String, Object> execute(Map<String, Object> b) {
                    if (fail) throw new IllegalStateException(name + " declined");
                    return b;
                }
                public void compensate(Compensation<Map<String, Object>, Map<String, Object>> c) { undone.add(name); }
            };
        }
        public CompensableActivity<Map<String, Object>, Map<String, Object>> reserve() { return activity("reserve", false); }
        public CompensableActivity<Map<String, Object>, Map<String, Object>> charge() { return activity("charge", chargeFails); }
        public Map<String, Object> ship(Map<String, Object> b) { return b; }
    }

    private static FlowSpec saga(String name) {
        return FlowSpec.define(name, 1, Map.class, BookingSteps.class, (f, s) -> f
                .thenApplyCompensable(s::reserve).thenApplyCompensable(s::charge).thenApply(s::ship));
    }

    @Test @DisplayName("the explicit API: a service reports steps by key, name and times, with no run object at all")
    void explicitApi() throws Exception {
        FlowSpec spec = spec("obsc-api");
        try (WiggleServer server = new WiggleServer(config()).start();
             Observer observer = Observer.connect(server.baseUrl(), ObserverOptions.defaults().withReporter("gateway"))) {
            ObservedFlow flow = observer.publish(spec);
            long t0 = System.currentTimeMillis() - 1000;
            flow.record("order-20", "a", t0, t0 + 10);
            flow.record("order-20", "b", t0 + 10, t0 + 50, "a");
            flow.recordPredicate("order-20", "keep", true, t0 + 60, t0 + 61);
            StepTimer c = flow.start("order-20", "c");
            Thread.sleep(15);
            c.done();
            assertThrows(IllegalArgumentException.class, () -> flow.record("order-20", "nope", t0, t0),
                    "an unknown step is refused here, not on the server");

            InstanceView v = awaitTerminal(server.engine(), spec.name(), "order-20");
            assertEquals("COMPLETED", v.status());
            assertTrue(server.engine().anomalies(null, v.id(), 10).isEmpty());
            Map<String, Token> done = new LinkedHashMap<>();
            for (Token t : server.engine().tokens(v.id())) if (t.startedAt != null) done.put(t.nodeId, t);
            assertEquals(4, done.size());
            String bNode = spec.definition().node(spec.definition().startNode()).next();
            assertEquals(40, done.get(bNode).finishedAt - done.get(bNode).startedAt, "the caller's own times are kept");
            assertEquals(spec.definition().startNode(), done.get(bNode).afterNode, "the hint names the step by name");
            assertTrue(done.get(spec.definition().node(bNode).next()).finishedAt - done.get(spec.definition().node(bNode).next()).startedAt < 15);
        }
    }

    @Test @DisplayName("the explicit API: a declared failure, then undos, end a saga COMPENSATED")
    void explicitApiCompensation() throws Exception {
        FlowSpec spec = saga("obsc-api-saga");
        try (WiggleServer server = new WiggleServer(config()).start();
             Observer inventory = Observer.connect(server.baseUrl(), ObserverOptions.defaults().withReporter("inventory"));
             Observer payments = Observer.connect(server.baseUrl(), ObserverOptions.defaults().withReporter("payments"))) {
            ObservedFlow inv = inventory.publish(spec);
            ObservedFlow pay = payments.publish(spec);
            long t0 = System.currentTimeMillis() - 1000;
            inv.record("order-21", "reserve", t0, t0 + 10);
            pay.recordError("order-21", "charge", "CardDeclined", t0 + 10, t0 + 20);
            InstanceView failing = await(() -> server.engine().findByCorrelation("order-21", 1).stream()
                    .filter(i -> "COMPENSATING".equals(i.status())).findFirst().orElse(null), Duration.ofSeconds(10));
            assertEquals("charge: CardDeclined", failing.error());
            assertThrows(IllegalArgumentException.class, () -> inv.recordUndo("order-21", "ship", t0, t0), "ship has no undo");
            StepTimer undo = inv.startUndo("order-21", "reserve");
            undo.done();
            InstanceView v = awaitTerminal(server.engine(), spec.name(), "order-21");
            assertEquals("COMPENSATED", v.status());
            assertTrue(server.engine().anomalies(null, v.id(), 10).isEmpty());
        }
    }

    @Test @DisplayName("instrumented: a compensable activity's execute and compensate are both reported")
    void instrumentedSaga() throws Exception {
        FlowSpec spec = saga("obsc-saga");
        Booking booking = new Booking();
        booking.chargeFails = true;
        try (WiggleServer server = new WiggleServer(config()).start();
             Observer observer = Observer.connect(server.baseUrl())) {
            Observed<BookingSteps> flow = observer.observe(spec, BookingSteps.class, booking);
            BookingSteps s = flow.steps();
            Map<String, Object> order = Map.of("id", "order-22");
            Run run = flow.begin("order-22");
            Map<String, Object> reserved = s.reserve().execute(order);
            assertThrows(IllegalStateException.class, () -> s.charge().execute(reserved), "the application's own exception");
            assertNull(flow.current(), "the throw ended the run on this side");

            // the application runs its undo, as it would on any platform, through the same wrapped activity
            try (Run again = flow.join("order-22")) {
                s.reserve().compensate(Compensations.of(order, reserved));
            }
            assertEquals(List.of("reserve"), booking.undone);

            InstanceView v = awaitTerminal(server.engine(), spec.name(), "order-22");
            assertEquals("COMPENSATED", v.status());
            assertEquals("charge: IllegalStateException: charge declined", v.error());
            assertTrue(server.engine().anomalies(null, v.id(), 10).isEmpty(), server.engine().anomalies(null, v.id(), 10).toString());
            String reserveNode = spec.definition().startNode();
            assertTrue(server.engine().tokens(v.id()).stream().anyMatch(t -> reserveNode.equals(t.undoOf) && t.startedAt != null),
                    "the undo is a timed token naming the step it reversed");
        }
    }

    @Test @DisplayName("run.fail declares a business failure from an open run")
    void runFail() throws Exception {
        FlowSpec spec = saga("obsc-runfail");
        Booking booking = new Booking();
        try (WiggleServer server = new WiggleServer(config()).start();
             Observer observer = Observer.connect(server.baseUrl())) {
            Observed<BookingSteps> flow = observer.observe(spec, BookingSteps.class, booking);
            Run run = flow.begin("order-23");
            flow.steps().reserve().execute(Map.of());
            run.fail("out of stock downstream");
            assertNull(flow.current());
            InstanceView failing = await(() -> server.engine().findByCorrelation("order-23", 1).stream()
                    .filter(i -> "COMPENSATING".equals(i.status())).findFirst().orElse(null), Duration.ofSeconds(10));
            assertEquals("out of stock downstream", failing.error());
        }
    }
}
