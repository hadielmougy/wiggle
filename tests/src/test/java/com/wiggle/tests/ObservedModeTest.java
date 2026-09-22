package com.wiggle.tests;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.core.AnomalyView;
import com.wiggle.core.ExecutionMode;
import com.wiggle.core.Node;
import com.wiggle.core.NodeStats;
import com.wiggle.core.ObserveResult;
import com.wiggle.core.WorkflowDefinition;
import com.wiggle.jdbc.JdbcStorage;
import com.wiggle.postgres.H2Dialect;
import com.wiggle.postgres.PostgresDialect;
import com.wiggle.proto.ListAnomaliesRequest;
import com.wiggle.proto.ObserveRunRequest;
import com.wiggle.proto.ObserveRunResult;
import com.wiggle.proto.StepResult;
import com.wiggle.proto.StepStatsRequest;
import com.wiggle.proto.WiggleControlPlaneGrpc;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import com.wiggle.server.engine.DefinitionRegistry;
import com.wiggle.server.engine.EngineException;
import com.wiggle.server.engine.WorkflowEngine;
import com.wiggle.server.engine.WorkflowEngine.StepInput;
import com.wiggle.server.store.InMemoryStorage;
import com.wiggle.server.store.Rows.Token;
import com.wiggle.server.store.Rows.TokenStatus;
import com.wiggle.server.store.Storage;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * OBSERVED execution: the server dispatches nothing; any number of instrumented applications
 * report the steps they ran under one key, the server appends them with their timings, and a
 * settle sweep judges the run against its topology once it has gone quiet.
 */
class ObservedModeTest {

    static {
        // A judged run needs no straggler grace here, and a quiet one should stall fast.
        System.setProperty("wiggle.observe.settleMillis", "0");
        System.setProperty("wiggle.observe.stallMillis", "300");
    }

    interface Steps {
        Map<String, Object> a(Map<String, Object> ctx);
        Map<String, Object> b(Map<String, Object> ctx);
        boolean keep(Map<String, Object> ctx);
        Map<String, Object> c(Map<String, Object> ctx);
    }

    private static final long T0 = 1_700_000_000_000L;
    private static final String APP1 = "app-1", APP2 = "app-2";

    /** A spec declares no mode of its own; OBSERVED is stamped on the published definition, as
     *  the observe module does. */
    private static FlowSpec observed(String name) {
        FlowSpec spec = FlowSpec.define(name, 1, Map.class, Steps.class, (f, s) -> f
                .thenApply(s::a)
                .thenApply(s::b)
                .thenFilter(s::keep)
                .thenApply(s::c));
        return new FlowSpec(observed(spec.definition()));
    }

    private static WorkflowDefinition observed(WorkflowDefinition d) {
        return new WorkflowDefinition(d.name(), d.version(), d.startNode(), d.nodes(), d.queues(),
                ExecutionMode.OBSERVED, d.checkpoints());
    }

    /** The node ids along {@code next} from the start: a, b, keep, c. */
    private static List<String> chain(WorkflowDefinition def) {
        List<String> ids = new ArrayList<>();
        for (String id = def.startNode(); def.node(id).isWorkerDispatched(); id = def.node(id).next()) ids.add(id);
        return ids;
    }

    private static StepInput step(String nodeId, long offset, long millis) {
        return new StepInput(nodeId, null, null, null, T0 + offset, T0 + offset + millis);
    }

    private static StepInput predicate(String nodeId, boolean value, long offset, long millis) {
        return new StepInput(nodeId, null, value, null, T0 + offset, T0 + offset + millis);
    }

    private static StepInput failed(String nodeId, String error, long offset, long millis) {
        return new StepInput(nodeId, null, null, error, T0 + offset, T0 + offset + millis);
    }

    private record Fixture(Storage storage, WorkflowEngine engine, FlowSpec spec, List<String> ids) implements AutoCloseable {
        static Fixture inMemory(String name) {
            return open(new InMemoryStorage(), name);
        }

        static Fixture open(Storage storage, String name) {
            storage.migrate();
            DefinitionRegistry registry = new DefinitionRegistry(storage);
            WorkflowEngine engine = new WorkflowEngine(storage, registry, 30_000);
            FlowSpec spec = observed(name);
            registry.register(spec.definition());
            return new Fixture(storage, engine, spec, chain(spec.definition()));
        }

        ObserveResult report(String reporter, String key, boolean fin, StepInput... steps) {
            return engine.observe(spec.name(), null, null, key, reporter, List.of(steps), fin);
        }

        /** The leader's settle sweep, run by hand. */
        int settle() { return engine.settleObservedRuns(100); }

        String status(String id) { return engine.instance(id).orElseThrow().status(); }

        List<AnomalyView> anomalies(String id) { return engine.anomalies(null, id, 20); }

        String a() { return ids.get(0); }
        String b() { return ids.get(1); }
        String keep() { return ids.get(2); }
        String c() { return ids.get(3); }

        Set<String> queues() { return spec.definition().queues(); }

        @Override public void close() { storage.close(); }
    }

    @Test @DisplayName("two reporters under one key land on one instance, nobody can poll it, and it completes at settle")
    void keyedRunAcrossReporters() {
        try (Fixture f = Fixture.inMemory("obs-keyed")) {
            ObserveResult first = f.report(APP1, "order-42", false, step(f.a(), 0, 10), step(f.b(), 10, 50));
            ObserveResult second = f.report(APP2, "order-42", false, predicate(f.keep(), true, 60, 1), step(f.c(), 61, 20));
            assertEquals(first.instanceId(), second.instanceId(), "the key names the run, whoever reports");
            assertEquals("RUNNING", second.instanceStatus(), "END seen, but not judged until it settles");
            assertTrue(f.engine.poll("w1", f.queues(), 10, null).isEmpty(), "an observed token is never offered");
            assertTrue(f.engine.tokens(first.instanceId()).stream().noneMatch(Token::isActive), "nothing is ever held");

            assertEquals(1, f.settle(), "one run was due");
            assertEquals("COMPLETED", f.status(first.instanceId()));
            assertEquals(0, f.settle(), "judged once");

            Map<String, Token> done = new LinkedHashMap<>();
            for (Token t : f.engine.tokens(first.instanceId())) {
                if (t.status == TokenStatus.DONE && t.startedAt != null) done.put(t.nodeId, t);
            }
            assertEquals(Set.of(f.a(), f.b(), f.keep(), f.c()), done.keySet(), "every reported step settled timed");
            assertEquals(50, done.get(f.b()).finishedAt - done.get(f.b()).startedAt);
            assertEquals(APP1, done.get(f.a()).leaseOwner, "each token names its reporter");
            assertEquals(APP2, done.get(f.c()).leaseOwner);
            assertEquals(first.instanceId(), f.engine.findByCorrelation("order-42", 1).getFirst().id(),
                    "the key is the instance's correlation id");

            List<NodeStats> stats = f.engine.stepStats(f.spec.name(), null, 0, 1000);
            assertEquals(f.b(), stats.getFirst().nodeId(), "slowest p95 first");
            assertEquals(50, stats.getFirst().p95Millis());
            assertTrue(f.anomalies(first.instanceId()).isEmpty());

            ObserveResult late = f.report(APP1, "order-42", false, step(f.c(), 90, 1));
            assertEquals(first.instanceId(), late.instanceId());
            assertEquals("COMPLETED", late.instanceStatus());
            assertEquals(1, late.anomalies());
            assertEquals("AFTER_END", f.anomalies(first.instanceId()).getFirst().kind());
        }
    }

    @Test @DisplayName("arrival order means nothing: steps reported late but timed in order raise nothing")
    void arrivalOrderIsIrrelevant() {
        try (Fixture f = Fixture.inMemory("obs-arrival")) {
            ObserveResult r = f.report(APP2, "k", false, step(f.c(), 61, 20));
            f.report(APP1, "k", false, step(f.a(), 0, 10), step(f.b(), 10, 50), predicate(f.keep(), true, 60, 1));
            f.settle();
            assertEquals("COMPLETED", f.status(r.instanceId()));
            assertTrue(f.anomalies(r.instanceId()).isEmpty(), "judged by the steps' own clocks");
        }
    }

    private static StepInput stepAfter(String nodeId, long offset, long millis, String cause) {
        return new StepInput(nodeId, null, null, null, T0 + offset, T0 + offset + millis, cause);
    }

    @Test @DisplayName("a second service whose clock runs early is ordered by the causal hint it carried")
    void causalHintOrdersAcrossServices() {
        try (Fixture f = Fixture.inMemory("obs-causal")) {
            // app-1 ran a, b, keep with a true clock; app-2's clock is 40ms behind, but its step names keep
            f.report(APP1, "k", false, step(f.a(), 0, 10), step(f.b(), 10, 10), predicate(f.keep(), true, 20, 1));
            ObserveResult r = f.report(APP2, "k", false, stepAfter(f.c(), -19, 5, f.keep()));
            f.settle();
            assertEquals("COMPLETED", f.status(r.instanceId()));
            assertTrue(f.anomalies(r.instanceId()).isEmpty(), f.anomalies(r.instanceId()).toString());
            Token c = f.engine.tokens(r.instanceId()).stream().filter(t -> t.nodeId.equals(f.c())).findFirst().orElseThrow();
            assertEquals(f.keep(), c.afterNode, "the hint is kept on the token");
        }
        try (Fixture f = Fixture.inMemory("obs-causal-none")) {
            f.report(APP1, "k", false, step(f.a(), 0, 10), step(f.b(), 10, 10), predicate(f.keep(), true, 20, 1));
            ObserveResult r = f.report(APP2, "k", false, step(f.c(), -19, 5));
            f.settle();
            List<String> kinds = f.anomalies(r.instanceId()).stream().map(AnomalyView::kind).toList();
            assertFalse(kinds.isEmpty(), "without the hint, the early clock reads as out of order");
            assertTrue(kinds.stream().allMatch("OUT_OF_ORDER"::equals), kinds.toString());
        }
    }

    @Test @DisplayName("steps whose clocks agree to the millisecond keep their reported order")
    void sameMillisecondKeepsReportOrder() {
        try (Fixture f = Fixture.inMemory("obs-tie")) {
            ObserveResult r = f.report(APP1, "k", false, step(f.a(), 0, 0), step(f.b(), 0, 0),
                    predicate(f.keep(), true, 0, 0), step(f.c(), 0, 0));
            f.settle();
            assertEquals("COMPLETED", f.status(r.instanceId()));
            assertTrue(f.anomalies(r.instanceId()).isEmpty(), "arrival order broke the tie, not token ids");
        }
    }

    @Test @DisplayName("a step timed out of order is recorded at settle, and the run still completes")
    void outOfOrderIsJudged() {
        try (Fixture f = Fixture.inMemory("obs-order")) {
            ObserveResult r = f.report(APP1, "k", false, step(f.a(), 0, 10), step(f.c(), 10, 20));
            assertEquals(0, r.anomalies(), "nothing is judged on arrival");
            f.settle();
            assertEquals("COMPLETED", f.status(r.instanceId()), "c's successor is END");
            List<AnomalyView> anomalies = f.anomalies(r.instanceId());
            assertEquals(1, anomalies.size());
            assertEquals("OUT_OF_ORDER", anomalies.getFirst().kind());
            assertEquals(f.b(), anomalies.getFirst().expectedNode());
            assertEquals(f.c(), anomalies.getFirst().reportedNode());
        }
    }

    @Test @DisplayName("at-least-once delivery: a step reported twice is a DUPLICATE, not a failure")
    void duplicateDelivery() {
        try (Fixture f = Fixture.inMemory("obs-dup")) {
            ObserveResult r = f.report(APP1, "k", false, step(f.a(), 0, 1), step(f.b(), 1, 1), step(f.b(), 2, 1),
                    predicate(f.keep(), true, 3, 1), step(f.c(), 4, 1));
            f.settle();
            assertEquals("COMPLETED", f.status(r.instanceId()));
            List<AnomalyView> anomalies = f.anomalies(r.instanceId());
            assertEquals(1, anomalies.size());
            assertEquals("DUPLICATE", anomalies.getFirst().kind());
            assertEquals(f.b(), anomalies.getFirst().reportedNode());
        }
    }

    @Test @DisplayName("a step the graph does not know is recorded at arrival and skipped")
    void unknownStepIsRecordedAndSkipped() {
        try (Fixture f = Fixture.inMemory("obs-unknown")) {
            ObserveResult r = f.report(APP1, "k", false, step(f.a(), 0, 1), step("nope", 1, 1),
                    step(f.b(), 2, 1), predicate(f.keep(), true, 3, 1), step(f.c(), 4, 1));
            assertEquals(1, r.anomalies());
            assertEquals("UNKNOWN_NODE", f.anomalies(r.instanceId()).getFirst().kind());
            f.settle();
            assertEquals("COMPLETED", f.status(r.instanceId()));
            assertEquals(1, f.anomalies(r.instanceId()).size(), "nothing else was found");
        }
    }

    @Test @DisplayName("a step that threw fails the run on the spot; later reports are anomalies")
    void thrownStepFailsTheInstance() {
        try (Fixture f = Fixture.inMemory("obs-error")) {
            ObserveResult r = f.report(APP1, "k", false, step(f.a(), 0, 1), failed(f.b(), "boom", 1, 5));
            assertEquals("FAILED", r.instanceStatus());
            assertEquals("b: boom", f.engine.instance(r.instanceId()).orElseThrow().error());
            Token b = f.engine.tokens(r.instanceId()).stream().filter(t -> t.nodeId.equals(f.b())).findFirst().orElseThrow();
            assertEquals(TokenStatus.FAILED, b.status);
            assertEquals("boom", b.lastError);
            assertEquals(5, b.finishedAt - b.startedAt, "a failed step is timed too");
            assertEquals(0, f.settle(), "a failed run is not due for judgement");

            ObserveResult late = f.report(APP2, "k", false, predicate(f.keep(), true, 10, 1));
            assertEquals("FAILED", late.instanceStatus());
            assertEquals("AFTER_END", f.anomalies(r.instanceId()).getFirst().kind());
        }
    }

    @Test @DisplayName("a run nobody finishes stalls: judged incomplete once it has gone quiet")
    void quietRunStalls() throws Exception {
        try (Fixture f = Fixture.inMemory("obs-stall")) {
            ObserveResult r = f.report(APP1, "k", false, step(f.a(), 0, 1));
            assertEquals(0, f.settle(), "still within the stall threshold");
            Thread.sleep(400);
            assertEquals(1, f.settle());
            assertEquals("FAILED", f.status(r.instanceId()));
            List<String> kinds = f.anomalies(r.instanceId()).stream().map(AnomalyView::kind).toList();
            assertTrue(kinds.contains("INCOMPLETE") && kinds.contains("STALLED"), kinds.toString());
            assertTrue(f.engine.instance(r.instanceId()).orElseThrow().error().contains("before END, at b"));
        }
    }

    @Test @DisplayName("a report marked final settles the run at once, incomplete but not stalled")
    void finalWithoutEndIsIncomplete() {
        try (Fixture f = Fixture.inMemory("obs-final")) {
            ObserveResult r = f.report(APP1, "k", true, step(f.a(), 0, 1));
            f.settle();
            assertEquals("FAILED", f.status(r.instanceId()));
            List<String> kinds = f.anomalies(r.instanceId()).stream().map(AnomalyView::kind).toList();
            assertEquals(List.of("INCOMPLETE"), kinds);
        }
    }

    @Test @DisplayName("a fork's branches reported by different services interleave without a finding")
    void forkJoinAcrossReporters() {
        try (Storage storage = new InMemoryStorage()) {
            DefinitionRegistry registry = new DefinitionRegistry(storage);
            WorkflowEngine engine = new WorkflowEngine(storage, registry, 30_000);
            Map<String, Node> n = new LinkedHashMap<>();
            n.put("a", Node.task("a", "a", "act", "q", null).withNext("f"));
            n.put("f", Node.fork("f", "f").withBranches(List.of("x", "y")));
            n.put("x", Node.task("x", "x", "act", "q", null).withNext("j"));
            n.put("y", Node.task("y", "y", "act", "q", null).withNext("j"));
            n.put("j", Node.join("j", "j", 2).withNext("z"));
            n.put("z", Node.task("z", "z", "act", "q", null).withNext("ok"));
            n.put("ok", Node.end("ok", true, "done"));
            registry.register(new WorkflowDefinition("obs-fork", 1, "a", n, Set.of("q"), ExecutionMode.OBSERVED));

            engine.observe("obs-fork", null, null, "k", "gateway", List.of(step("a", 0, 5)), false);
            engine.observe("obs-fork", null, null, "k", "inventory", List.of(step("y", 6, 40)), false);
            engine.observe("obs-fork", null, null, "k", "payments", List.of(step("x", 5, 30)), false);
            ObserveResult r = engine.observe("obs-fork", null, null, "k", "gateway", List.of(step("z", 50, 5)), false);
            engine.settleObservedRuns(10);
            assertEquals("COMPLETED", engine.instance(r.instanceId()).orElseThrow().status());
            assertTrue(engine.anomalies(null, r.instanceId(), 10).isEmpty());
            assertEquals(3, engine.tokens(r.instanceId()).stream().map(t -> t.leaseOwner).filter(o -> o != null).distinct().count(),
                    "three services contributed");
        }
    }

    @Test @DisplayName("registration refuses an OBSERVED graph the server would have to run part of")
    void registrationRefusesUnobservableGraphs() {
        try (Storage storage = new InMemoryStorage()) {
            DefinitionRegistry registry = new DefinitionRegistry(storage);

            Map<String, Node> withSleep = new LinkedHashMap<>();
            withSleep.put("t", Node.task("t", "t", "act", "q", null).withNext("s"));
            withSleep.put("s", Node.sleep("s", "nap", 10).withNext("ok"));
            withSleep.put("ok", Node.end("ok", true, "done"));
            EngineException e = assertThrows(EngineException.class, () -> registry.register(
                    new WorkflowDefinition("obs-sleep", 1, "t", withSleep, Set.of("q"), ExecutionMode.OBSERVED)));
            assertEquals(400, e.statusCode());
            assertTrue(e.getMessage().contains("SLEEP"), e.getMessage());

            Map<String, Node> withUndo = new LinkedHashMap<>();
            withUndo.put("t", Node.task("t", "t", "act", "q", null).withNext("ok").withCompensable());
            withUndo.put("ok", Node.end("ok", true, "done"));
            assertDoesNotThrow(() -> registry.register(
                    new WorkflowDefinition("obs-undo", 1, "t", withUndo, Set.of("q"), ExecutionMode.OBSERVED)),
                    "a compensable step is observable: its undo is reported like any step");

            assertDoesNotThrow(() -> registry.register(
                    new WorkflowDefinition("srv-sleep", 1, "t", withSleep, Set.of("q"), ExecutionMode.SERVER)),
                    "the same graph is fine when a worker runs it");
        }
    }

    @Test @DisplayName("a report against a workflow workers run is refused, not applied")
    void observeRefusesWorkerRunWorkflows() {
        try (Storage storage = new InMemoryStorage()) {
            DefinitionRegistry registry = new DefinitionRegistry(storage);
            WorkflowEngine engine = new WorkflowEngine(storage, registry, 30_000);
            FlowSpec server = FlowSpec.define("srv-linear", 1, Map.class, Steps.class, (f, s) -> f
                    .executeInServer().thenApply(s::a).thenApply(s::b));
            registry.register(server.definition());
            EngineException e = assertThrows(EngineException.class, () -> engine.observe(server.name(), null,
                    null, "k", APP1, List.of(step(server.definition().startNode(), 0, 1)), false));
            assertEquals(400, e.statusCode());
            assertTrue(engine.list(server.name(), null, 10).isEmpty(), "nothing was started");

            assertThrows(EngineException.class, () -> engine.observe("no-such", null, null, "k", APP1,
                    List.of(step("x", 0, 1)), false));
        }
    }

    @Test @DisplayName("over gRPC: ObserveRun, GetStepStats and ListAnomalies, with timings on the instance's tokens")
    void overGrpc() throws Exception {
        ServerConfig config = new ServerConfig(TestPorts.free(), "obs-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
        FlowSpec spec = observed("obs-grpc");
        List<String> ids = chain(spec.definition());
        try (WiggleServer server = new WiggleServer(config).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            client.register(spec);
            ManagedChannel channel = Grpc.newChannelBuilder(server.baseUrl(), InsecureChannelCredentials.create()).build();
            try {
                WiggleControlPlaneGrpc.WiggleControlPlaneBlockingStub stub = WiggleControlPlaneGrpc.newBlockingStub(channel);
                ObserveRunResult first = stub.observeRun(ObserveRunRequest.newBuilder()
                        .setWorkflow(spec.name()).setReporter(APP1).setCorrelationId("order-1")
                        .addSteps(StepResult.newBuilder().setNodeId(ids.get(0)).setStartedAt(T0).setFinishedAt(T0 + 10))
                        .addSteps(StepResult.newBuilder().setNodeId(ids.get(3)).setStartedAt(T0 + 10).setFinishedAt(T0 + 40))
                        .build());
                assertEquals("RUNNING", first.getInstanceStatus());
                assertFalse(first.getInstanceId().isEmpty());
                assertEquals(first.getInstanceId(), client.findByCorrelation("order-1", 10).getFirst().id());

                var stats = stub.getStepStats(StepStatsRequest.newBuilder().setWorkflow(spec.name()).build());
                assertEquals(2, stats.getNodesCount(), "timings are there before judgement");
                assertEquals(ids.get(3), stats.getNodes(0).getNodeId());
                assertEquals(30, stats.getNodes(0).getP95Millis());

                server.engine().settleObservedRuns(10);
                assertEquals("COMPLETED", client.findByCorrelation("order-1", 10).getFirst().status());
                var anomalies = stub.listAnomalies(ListAnomaliesRequest.newBuilder().setWorkflow(spec.name()).build());
                assertEquals(1, anomalies.getAnomaliesCount());
                assertEquals("OUT_OF_ORDER", anomalies.getAnomalies(0).getKind());

                var detail = stub.getInstance(com.wiggle.proto.InstanceIdRequest.newBuilder()
                        .setInstanceId(first.getInstanceId()).build());
                assertTrue(detail.getTokensList().stream().anyMatch(t -> t.getNodeId().equals(ids.get(3))
                        && t.getStartedAt() == T0 + 10 && t.getFinishedAt() == T0 + 40), "timings reach the API");
            } finally {
                channel.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
            }
        }
    }

    @Test @DisplayName("on the JDBC store: keyed creation, timings, settle and anomalies round-trip")
    void onJdbc() {
        String url = TestStorage.url("obs");
        var dialect = url.startsWith("jdbc:postgresql") ? new PostgresDialect() : new H2Dialect();
        String name = "obs-jdbc-" + System.nanoTime();
        try (Fixture f = Fixture.open(new JdbcStorage(url, TestStorage.user(), TestStorage.password(), 4, dialect), name)) {
            ObserveResult r1 = f.report(APP1, "k", false, step(f.a(), 0, 10));
            ObserveResult r2 = f.report(APP2, "k", false, step(f.c(), 10, 20));
            assertEquals(r1.instanceId(), r2.instanceId(), "keyed creation converges on the database");
            assertEquals("RUNNING", f.status(r1.instanceId()));
            assertEquals(1, f.settle());
            assertEquals("COMPLETED", f.status(r1.instanceId()));

            List<NodeStats> stats = f.engine.stepStats(name, null, 0, 1000);
            assertEquals(2, stats.size());
            assertEquals(f.c(), stats.getFirst().nodeId());
            assertEquals(20, stats.getFirst().p50Millis());

            List<AnomalyView> anomalies = f.engine.anomalies(name, null, 10);
            assertEquals(1, anomalies.size());
            assertEquals("OUT_OF_ORDER", anomalies.getFirst().kind());
            assertEquals(f.b(), anomalies.getFirst().expectedNode());
            assertEquals("expected b, got c", anomalies.getFirst().detail());
            assertTrue(f.engine.anomalies("other", null, 10).isEmpty());

            Token c = f.engine.tokens(r1.instanceId()).stream().filter(t -> t.nodeId.equals(f.c())).findFirst().orElseThrow();
            assertEquals(T0 + 10, c.startedAt);
            assertEquals(APP2, c.leaseOwner);
            assertNull(c.afterNode);
            assertNull(c.undoOf);
            ObserveResult r3 = f.report(APP1, "k2", false, stepAfter(f.a(), 0, 1, null), stepAfter(f.b(), 1, 1, f.a()));
            assertEquals(f.a(), f.engine.tokens(r3.instanceId()).stream().filter(t -> t.nodeId.equals(f.b()))
                    .findFirst().orElseThrow().afterNode, "the hint round-trips through the store");
            assertNull(f.engine.instance(r1.instanceId()).orElseThrow().error());
            assertTrue(f.engine.poll("w1", f.queues(), 10, null).isEmpty());
        }
    }

    /** reserve(undo) -> charge(undo) -> ship -> ok, registered OBSERVED on a fresh engine. */
    private record Saga(Storage storage, WorkflowEngine engine) implements AutoCloseable {
        static Saga open() {
            Storage storage = new InMemoryStorage();
            DefinitionRegistry registry = new DefinitionRegistry(storage);
            WorkflowEngine engine = new WorkflowEngine(storage, registry, 30_000);
            Map<String, Node> n = new LinkedHashMap<>();
            n.put("reserve", Node.task("reserve", "reserve", "act", "q", null).withNext("charge").withCompensable());
            n.put("charge", Node.task("charge", "charge", "act", "q", null).withNext("ship").withCompensable());
            n.put("ship", Node.task("ship", "ship", "act", "q", null).withNext("ok"));
            n.put("ok", Node.end("ok", true, "done"));
            registry.register(new WorkflowDefinition("obs-saga", 1, "reserve", n, Set.of("q"), ExecutionMode.OBSERVED));
            return new Saga(storage, engine);
        }

        ObserveResult report(String reporter, String failure, StepInput... steps) {
            return engine.observe("obs-saga", null, null, "k", reporter, List.of(steps), false, failure);
        }

        String status(String id) { return engine.instance(id).orElseThrow().status(); }
        List<String> kinds(String id) { return engine.anomalies(null, id, 20).stream().map(AnomalyView::kind).toList(); }

        @Override public void close() { storage.close(); }
    }

    private static StepInput undo(String nodeId, long offset, long millis) {
        return StepInput.undo(nodeId, null, T0 + offset, T0 + offset + millis, null);
    }

    @Test @DisplayName("a declared failure enters the reverse pass; the services' undos land and the run ends COMPENSATED")
    void declaredFailureThenUndos() {
        try (Saga s = Saga.open()) {
            s.report("inventory", null, step("reserve", 0, 10));
            ObserveResult r = s.report("payments", "card declined", step("charge", 10, 10));
            assertEquals("COMPENSATING", r.instanceStatus(), "the graph has undos: the reverse pass is open");
            assertEquals("card declined", s.engine.instance(r.instanceId()).orElseThrow().error());
            assertEquals(0, s.engine.settleObservedRuns(10), "undos outstanding: not due yet");

            s.report("payments", null, undo("charge", 30, 5));
            assertEquals(0, s.engine.settleObservedRuns(10), "one undo still owed");
            ObserveResult last = s.report("inventory", null, undo("reserve", 40, 5));
            assertEquals("COMPENSATING", last.instanceStatus());
            assertEquals(1, s.engine.settleObservedRuns(10), "every undo landed: judged after the short grace");
            assertEquals("COMPENSATED", s.status(r.instanceId()));
            assertTrue(s.kinds(r.instanceId()).isEmpty(), s.kinds(r.instanceId()).toString());

            Token undoTok = s.engine.tokens(r.instanceId()).stream().filter(t -> "charge".equals(t.undoOf)).findFirst().orElseThrow();
            assertEquals(TokenStatus.DONE, undoTok.status);
            assertEquals("payments", undoTok.leaseOwner);
            assertTrue(undoTok.activity.endsWith("#compensate"), "an undo token is named as the saga path names one");
            List<NodeStats> stats = s.engine.stepStats("obs-saga", null, 0, 100);
            assertTrue(stats.stream().anyMatch(n -> n.nodeId().equals("charge#compensate") && n.name().equals("charge (undo)")),
                    "undo durations are kept apart from the step's: " + stats);
        }
    }

    @Test @DisplayName("undos still outstanding when the run goes quiet end it COMPENSATION_FAILED")
    void missingUndoStalls() throws Exception {
        try (Saga s = Saga.open()) {
            ObserveResult r = s.report("inventory", "boom", step("reserve", 0, 10), step("charge", 10, 10));
            s.report("payments", null, undo("charge", 30, 5));
            Thread.sleep(400);
            assertEquals(1, s.engine.settleObservedRuns(10));
            assertEquals("COMPENSATION_FAILED", s.status(r.instanceId()));
            List<String> kinds = s.kinds(r.instanceId());
            assertTrue(kinds.contains("MISSING_UNDO") && kinds.contains("STALLED"), kinds.toString());
            assertTrue(s.engine.instance(r.instanceId()).orElseThrow().error().contains("undo of reserve never reported"));
        }
    }

    @Test @DisplayName("a thrown step declares the failure; a graph with nothing to undo fails in place")
    void thrownStepAndPlainGraph() {
        try (Saga s = Saga.open()) {
            ObserveResult r = s.report("inventory", null, step("reserve", 0, 10), failed("charge", "gateway down", 10, 5));
            assertEquals("COMPENSATING", r.instanceStatus(), "charge threw after reserve completed: reserve's undo is owed");
            assertEquals("charge: gateway down", s.engine.instance(r.instanceId()).orElseThrow().error());
            s.report("inventory", null, undo("reserve", 20, 5));
            s.engine.settleObservedRuns(10);
            assertEquals("COMPENSATED", s.status(r.instanceId()));
        }
        try (Fixture f = Fixture.inMemory("obs-plain")) {
            ObserveResult r = f.engine.observe(f.spec.name(), null, null, "k", APP1, List.of(step(f.a(), 0, 1)), false, "no stock");
            assertEquals("FAILED", r.instanceStatus(), "nothing compensable in the graph: fails in place");
            assertEquals("no stock", f.engine.instance(r.instanceId()).orElseThrow().error());
        }
    }

    @Test @DisplayName("an undo reported in a run that never failed is a finding, and the run still completes")
    void undoWithoutFailure() {
        try (Saga s = Saga.open()) {
            ObserveResult r = s.report("inventory", null, step("reserve", 0, 1), step("charge", 1, 1), undo("charge", 2, 1), step("ship", 3, 1));
            assertEquals("RUNNING", r.instanceStatus());
            s.engine.settleObservedRuns(10);
            assertEquals("COMPLETED", s.status(r.instanceId()));
            assertEquals(List.of("UNDO_WITHOUT_FAILURE"), s.kinds(r.instanceId()));
            ObserveResult bad = s.report("inventory", null, undo("ship", 4, 1));
            assertTrue(s.kinds(r.instanceId()).contains("UNDO_WITHOUT_STEP"), "ship declares no undo: " + s.kinds(r.instanceId()));
        }
    }

    @Test @DisplayName("a step getting slower over its runs is a DEGRADING anomaly, once per cooldown, naming the run that tipped it")
    void degradingStep() {
        System.setProperty("wiggle.observe.drift.window", "5");
        System.setProperty("wiggle.observe.drift.baseline", "10");
        try (Fixture f = Fixture.inMemory("obs-drift")) {
            assertEquals(0, f.engine.detectDegradation(), "nothing timed yet");
            // 10 runs where b takes ~10 ms, then 5 where it takes ~60 ms; a takes 5 ms throughout
            String tipping = null;
            for (int i = 0; i < 15; i++) {
                long bMillis = i < 10 ? 10 + (i % 3) : 60 + (i % 3);
                long base = i * 1000L;
                ObserveResult r = f.report(APP1, "run-" + i, false, step(f.a(), base, 5), step(f.b(), base + 5, bMillis),
                        predicate(f.keep(), true, base + 100, 1), step(f.c(), base + 101, 1));
                tipping = r.instanceId();
            }
            f.settle();
            assertEquals(1, f.engine.detectDegradation(), "b drifted; a, keep and c did not");
            List<AnomalyView> found = f.engine.anomalies(f.spec.name(), null, 50).stream()
                    .filter(a -> a.kind().equals("DEGRADING")).toList();
            assertEquals(1, found.size());
            assertEquals(f.b(), found.getFirst().reportedNode());
            assertEquals(tipping, found.getFirst().instanceId(), "linked to the newest run of the step");
            assertTrue(found.getFirst().detail().startsWith("b: p50 61 ms over the last 5 runs, 11 ms over the 10 before"), found.getFirst().detail());
            assertEquals(0, f.engine.detectDegradation(), "silenced for the cooldown");
        } finally {
            System.clearProperty("wiggle.observe.drift.window");
            System.clearProperty("wiggle.observe.drift.baseline");
        }
    }
}
