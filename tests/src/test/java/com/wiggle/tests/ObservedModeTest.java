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
 * OBSERVED execution: the server dispatches nothing, an instrumented application reports the
 * steps it ran, and the server keeps their timings and records every departure from the topology
 * rather than refusing it.
 */
class ObservedModeTest {

    interface Steps {
        Map<String, Object> a(Map<String, Object> ctx);
        Map<String, Object> b(Map<String, Object> ctx);
        boolean keep(Map<String, Object> ctx);
        Map<String, Object> c(Map<String, Object> ctx);
    }

    private static final long T0 = 1_700_000_000_000L;
    private static final String REPORTER = "app-1";

    /** A spec declares no mode of its own; OBSERVED is stamped on the published definition, as
     *  the observe module does. */
    private static FlowSpec observed(String name) {
        FlowSpec spec = FlowSpec.define(name, 1, Map.class, Steps.class, (f, s) -> f
                .thenApply(s::a)
                .thenApply(s::b)
                .thenFilter(s::keep)
                .thenApply(s::c));
        WorkflowDefinition d = spec.definition();
        return new FlowSpec(new WorkflowDefinition(d.name(), d.version(), d.startNode(), d.nodes(), d.queues(),
                ExecutionMode.OBSERVED, d.checkpoints()));
    }

    /** The node ids along {@code next} from the start: a, b, keep, c. */
    private static List<String> chain(WorkflowDefinition def) {
        List<String> ids = new ArrayList<>();
        for (String id = def.startNode(); def.node(id).isWorkerDispatched(); id = def.node(id).next()) ids.add(id);
        return ids;
    }

    /** A timed step: {@code offset} millis after T0, lasting {@code millis}. */
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

        ObserveResult report(String instanceId, boolean fin, StepInput... steps) {
            return engine.observe(spec.name(), null, instanceId, "order-42", REPORTER, List.of(steps), fin);
        }

        String a() { return ids.get(0); }
        String b() { return ids.get(1); }
        String keep() { return ids.get(2); }
        String c() { return ids.get(3); }

        Set<String> queues() { return spec.definition().queues(); }

        @Override public void close() { storage.close(); }
    }

    @Test @DisplayName("a reported run maps to an instance nobody can poll, keeps its timings, and completes at END")
    void reportedRunCompletesWithTimings() {
        try (Fixture f = Fixture.inMemory("obs-linear")) {
            ObserveResult first = f.report(null, false, step(f.a(), 0, 10), step(f.b(), 10, 50));
            assertNotNull(first.instanceId());
            assertTrue(first.running(), "two of four steps in: still running");
            assertEquals(0, first.anomalies());
            assertTrue(f.engine.poll("w1", f.queues(), 10, null).isEmpty(), "an observed token is never offered");
            Token held = f.engine.tokens(first.instanceId()).stream().filter(Token::isActive).findFirst().orElseThrow();
            assertEquals(f.keep(), held.nodeId, "waiting where the run left off");
            assertEquals(TokenStatus.RUNNING, held.status);
            assertEquals(REPORTER, held.leaseOwner, "held by the reporter, never READY");

            ObserveResult last = f.report(first.instanceId(), true,
                    predicate(f.keep(), true, 60, 1), step(f.c(), 61, 20));
            assertEquals("COMPLETED", last.instanceStatus());
            assertEquals(0, last.anomalies());

            Map<String, Token> done = new LinkedHashMap<>();
            for (Token t : f.engine.tokens(first.instanceId())) {
                if (t.status == TokenStatus.DONE && t.startedAt != null) done.put(t.nodeId, t);
            }
            assertEquals(Set.of(f.a(), f.b(), f.keep(), f.c()), done.keySet(), "every reported step settled timed");
            assertEquals(50, done.get(f.b()).finishedAt - done.get(f.b()).startedAt);

            List<NodeStats> stats = f.engine.stepStats(f.spec.name(), null, 0, 1000);
            assertEquals(4, stats.size());
            assertEquals(f.b(), stats.getFirst().nodeId(), "slowest p95 first");
            assertEquals(50, stats.getFirst().p95Millis());
            assertEquals(1, stats.getFirst().count());
            assertEquals("b", stats.getFirst().name());
            assertTrue(f.engine.anomalies(f.spec.name(), null, 10).isEmpty());
        }
    }

    @Test @DisplayName("a step reported out of order is recorded and the run resynchronised to it")
    void outOfOrderIsRecordedAndResynced() {
        try (Fixture f = Fixture.inMemory("obs-order")) {
            ObserveResult r = f.report(null, false, step(f.a(), 0, 10), step(f.c(), 10, 20));
            assertEquals(1, r.anomalies());
            assertEquals("COMPLETED", r.instanceStatus(), "c is the last step, so END was reached");

            List<AnomalyView> anomalies = f.engine.anomalies(null, r.instanceId(), 10);
            assertEquals(1, anomalies.size());
            AnomalyView a = anomalies.getFirst();
            assertEquals("OUT_OF_ORDER", a.kind());
            assertEquals(f.b(), a.expectedNode());
            assertEquals(f.c(), a.reportedNode());
            assertEquals(r.instanceId(), a.instanceId());

            assertTrue(f.engine.tokens(r.instanceId()).stream()
                    .anyMatch(t -> t.nodeId.equals(f.b()) && t.status == TokenStatus.CANCELLED),
                    "the token waiting at b was abandoned, not settled");
            assertTrue(f.engine.tokens(r.instanceId()).stream()
                    .anyMatch(t -> t.nodeId.equals(f.c()) && t.status == TokenStatus.DONE && t.startedAt != null),
                    "c still yields its timing");
        }
    }

    @Test @DisplayName("a step the graph does not know is recorded and skipped")
    void unknownStepIsRecordedAndSkipped() {
        try (Fixture f = Fixture.inMemory("obs-unknown")) {
            ObserveResult r = f.report(null, true, step(f.a(), 0, 1), step("nope", 1, 1),
                    step(f.b(), 2, 1), predicate(f.keep(), true, 3, 1), step(f.c(), 4, 1));
            assertEquals(1, r.anomalies());
            assertEquals("COMPLETED", r.instanceStatus());
            AnomalyView a = f.engine.anomalies(null, r.instanceId(), 10).getFirst();
            assertEquals("UNKNOWN_NODE", a.kind());
            assertEquals("nope", a.reportedNode());
            assertEquals(f.b(), a.expectedNode());
        }
    }

    @Test @DisplayName("a step that threw fails the instance; anything reported after that is an anomaly")
    void thrownStepFailsTheInstance() {
        try (Fixture f = Fixture.inMemory("obs-error")) {
            ObserveResult r = f.report(null, false, step(f.a(), 0, 1), failed(f.b(), "boom", 1, 5));
            assertEquals("FAILED", r.instanceStatus());
            assertEquals(0, r.anomalies(), "a thrown step is an outcome, not a reporting defect");
            assertEquals("b: boom", f.engine.instance(r.instanceId()).orElseThrow().error());
            Token b = f.engine.tokens(r.instanceId()).stream().filter(t -> t.nodeId.equals(f.b())).findFirst().orElseThrow();
            assertEquals(TokenStatus.FAILED, b.status);
            assertEquals("boom", b.lastError);
            assertEquals(5, b.finishedAt - b.startedAt, "a failed step is timed too");

            ObserveResult late = f.report(r.instanceId(), true, predicate(f.keep(), true, 10, 1));
            assertEquals("FAILED", late.instanceStatus());
            assertEquals(1, late.anomalies());
            assertEquals("AFTER_END", f.engine.anomalies(null, r.instanceId(), 10).getFirst().kind());
        }
    }

    @Test @DisplayName("a run that closes before END fails the instance as incomplete")
    void closingBeforeEndIsIncomplete() {
        try (Fixture f = Fixture.inMemory("obs-incomplete")) {
            ObserveResult r = f.report(null, true, step(f.a(), 0, 1));
            assertEquals("FAILED", r.instanceStatus());
            assertEquals(1, r.anomalies());
            AnomalyView a = f.engine.anomalies(f.spec.name(), null, 10).getFirst();
            assertEquals("INCOMPLETE", a.kind());
            assertEquals(f.b(), a.expectedNode(), "the run stopped where b was due");
            assertTrue(f.engine.instance(r.instanceId()).orElseThrow().error().contains("before END"));
        }
    }

    @Test @DisplayName("a predicate's reported value picks its branch")
    void predicateValueRoutes() {
        try (Fixture f = Fixture.inMemory("obs-branch")) {
            Node keep = f.spec.definition().node(f.keep());
            ObserveResult r = f.report(null, false, step(f.a(), 0, 1), step(f.b(), 1, 1),
                    predicate(f.keep(), false, 2, 1));
            Node alt = f.spec.definition().node(keep.altNext());
            String expected = alt.isWorkerDispatched() ? "RUNNING" : (alt.success() ? "COMPLETED" : "FAILED");
            assertEquals(expected, r.instanceStatus(), "the false branch of a filter was taken");
            assertEquals(0, r.anomalies());
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
            e = assertThrows(EngineException.class, () -> registry.register(
                    new WorkflowDefinition("obs-undo", 1, "t", withUndo, Set.of("q"), ExecutionMode.OBSERVED)));
            assertEquals(400, e.statusCode());
            assertTrue(e.getMessage().contains("compensable"), e.getMessage());

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
                    null, null, REPORTER, List.of(step(server.definition().startNode(), 0, 1)), false));
            assertEquals(400, e.statusCode());
            assertTrue(f(engine.list(server.name(), null, 10)).isEmpty(), "nothing was started");

            assertThrows(EngineException.class, () -> engine.observe("no-such", null, null, null, REPORTER,
                    List.of(step("x", 0, 1)), false));
        }
    }

    private static <T> List<T> f(List<T> l) { return l; }

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
                        .setWorkflow(spec.name()).setReporter(REPORTER).setCorrelationId("order-1")
                        .addSteps(StepResult.newBuilder().setNodeId(ids.get(0)).setStartedAt(T0).setFinishedAt(T0 + 10))
                        .addSteps(StepResult.newBuilder().setNodeId(ids.get(3)).setStartedAt(T0 + 10).setFinishedAt(T0 + 40))
                        .build());
                assertEquals("COMPLETED", first.getInstanceStatus());
                assertEquals(1, first.getAnomalies(), "c reported where b was due");
                assertFalse(first.getInstanceId().isEmpty());

                var stats = stub.getStepStats(StepStatsRequest.newBuilder().setWorkflow(spec.name()).build());
                assertEquals(2, stats.getNodesCount());
                assertEquals(ids.get(3), stats.getNodes(0).getNodeId());
                assertEquals(30, stats.getNodes(0).getP95Millis());

                var anomalies = stub.listAnomalies(ListAnomaliesRequest.newBuilder().setWorkflow(spec.name()).build());
                assertEquals(1, anomalies.getAnomaliesCount());
                assertEquals("OUT_OF_ORDER", anomalies.getAnomalies(0).getKind());
                assertEquals(first.getInstanceId(), anomalies.getAnomalies(0).getInstanceId());

                var detail = stub.getInstance(com.wiggle.proto.InstanceIdRequest.newBuilder()
                        .setInstanceId(first.getInstanceId()).build());
                assertTrue(detail.getTokensList().stream().anyMatch(t -> t.getNodeId().equals(ids.get(3))
                        && t.getStartedAt() == T0 + 10 && t.getFinishedAt() == T0 + 40), "timings reach the API");
                assertEquals(first.getInstanceId(), client.findByCorrelation("order-1", 10).getFirst().id());
            } finally {
                channel.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
            }
        }
    }

    @Test @DisplayName("on the JDBC store: the migration lands, timings and anomalies round-trip")
    void onJdbc() {
        String url = TestStorage.url("obs");
        var dialect = url.startsWith("jdbc:postgresql") ? new PostgresDialect() : new H2Dialect();
        String name = "obs-jdbc-" + System.nanoTime();
        try (Fixture f = Fixture.open(new JdbcStorage(url, TestStorage.user(), TestStorage.password(), 4, dialect), name)) {
            ObserveResult r = f.report(null, false, step(f.a(), 0, 10), step(f.c(), 10, 20));
            assertEquals("COMPLETED", r.instanceStatus());
            assertEquals(1, r.anomalies());

            List<NodeStats> stats = f.engine.stepStats(name, null, 0, 1000);
            assertEquals(2, stats.size());
            assertEquals(f.c(), stats.getFirst().nodeId());
            assertEquals(20, stats.getFirst().p50Millis());
            assertEquals(10, stats.get(1).maxMillis());

            List<AnomalyView> anomalies = f.engine.anomalies(name, null, 10);
            assertEquals(1, anomalies.size());
            assertEquals("OUT_OF_ORDER", anomalies.getFirst().kind());
            assertEquals(f.b(), anomalies.getFirst().expectedNode());
            assertEquals("expected b, got c", anomalies.getFirst().detail());
            assertEquals(1, f.engine.anomalies(null, r.instanceId(), 10).size());
            assertTrue(f.engine.anomalies("other", null, 10).isEmpty());

            Token c = f.engine.tokens(r.instanceId()).stream().filter(t -> t.nodeId.equals(f.c())).findFirst().orElseThrow();
            assertEquals(T0 + 10, c.startedAt);
            assertEquals(T0 + 30, c.finishedAt);
            assertTrue(f.engine.poll("w1", f.queues(), 10, null).isEmpty());
        }
    }
}
