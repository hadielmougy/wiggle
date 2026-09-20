package com.wiggle.server.engine;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.flow.Wiggle;
import com.wiggle.client.worker.CompensableActivity;
import com.wiggle.core.ExecutionMode;
import com.wiggle.core.GraphTraversal;
import com.wiggle.core.InstanceView;
import com.wiggle.core.Json;
import com.wiggle.core.Node;
import com.wiggle.core.TaskActivation;
import com.wiggle.server.engine.WorkflowEngine.AdvanceOutcome;
import com.wiggle.server.engine.WorkflowEngine.Run;
import com.wiggle.server.engine.WorkflowEngine.RunResult;
import com.wiggle.server.engine.WorkflowEngine.StepInput;
import com.wiggle.jdbc.JdbcStorage;
import com.wiggle.postgres.H2Dialect;
import com.wiggle.server.store.InMemoryStorage;
import com.wiggle.server.store.Storage;
import com.wiggle.server.store.Tx;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The cross-instance batch, driven directly: a real worker in LOCAL_ASYNC reaches
 * {@code advanceMany} through its own buffering, so no end-to-end test exercises the batch
 * seams -- validate refusing without writing, the one-commit apply, and the rollback-and-replay
 * path -- and each is pinned here instead.
 */
class AdvanceManyTest {

    /** The steps these specs name; a worker binds them by name. */
    interface TwoSteps {
        Map<String, Object> x(Map<String, Object> ctx);
        Map<String, Object> y(Map<String, Object> ctx);
    }

    interface ForkSteps {
        Map<String, Object> a(Map<String, Object> ctx);
        Map<String, Object> b(Map<String, Object> ctx);
        Map<String, Object> pick(Map<String, Object> left, Map<String, Object> right);
    }

    interface LoopSteps {
        boolean forever(Map<String, Object> ctx);
        Map<String, Object> spin(Map<String, Object> ctx);
        Map<String, Object> after(Map<String, Object> ctx);
    }

    interface SagaSteps {
        CompensableActivity<Map<String, Object>, Map<String, Object>> reserve();
        Map<String, Object> boom(Map<String, Object> ctx);
    }

    private static FlowSpec linear(String name, ExecutionMode mode) {
        return FlowSpec.define(name, 1, Map.class, TwoSteps.class, (f, s) -> f
                .execution(mode)
                .thenApply(s::x)
                .thenApply(s::y));
    }

    /** The whole two-step run, exactly as a LOCAL_ASYNC worker would flush it. */
    private static Run fullRun(FlowSpec bp, TaskActivation first, String owner) {
        String yNode = bp.definition().node(first.nodeId()).next();
        return new Run(first.taskId(), owner, List.of(
                new StepInput(first.nodeId(), Map.of("x", 1L), null),
                new StepInput(yNode, Map.of("x", 1L, "y", 2L), null)), true);
    }

    @Test @DisplayName("one call, one commit: every instance in the batch advances independently")
    void batchAdvancesIndependentInstances() {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            DefinitionRegistry registry = new DefinitionRegistry(storage);
            WorkflowEngine engine = new WorkflowEngine(storage, registry, 30_000);
            FlowSpec bp = linear("am-linear", ExecutionMode.LOCAL_ASYNC);
            registry.register(bp.definition());
            Set<String> queues = bp.definition().queues();
            for (int i = 0; i < 3; i++) engine.start(bp.name(), bp.version(), Map.of(), null);
            List<TaskActivation> claimed = engine.poll("w1", queues, 10, null);
            assertEquals(3, claimed.size());

            Map<String, RunResult> results = engine.advanceMany(
                    claimed.stream().map(t -> fullRun(bp, t, "w1")).toList());

            assertEquals(3, results.size());
            for (TaskActivation t : claimed) {
                RunResult r = results.get(t.taskId());
                assertTrue(r.ok(), t.taskId() + ": " + r);
                assertEquals("COMPLETED", r.outcome().instanceStatus());
                Map<String, Object> ctx = Json.asObject(engine.instance(t.instanceId()).orElseThrow().context());
                assertEquals(1L, ctx.get("x"));
                assertEquals(2L, ctx.get("y"));
            }
            assertTrue(engine.poll("w2", queues, 10, null).isEmpty(), "nothing left to dispatch");
        }
    }

    @Test @DisplayName("a refused run costs its batch-mates nothing: one transaction, no replay, still reportable")
    void refusedRunLeavesBatchMatesAlone() {
        CountingStorage storage = new CountingStorage(new InMemoryStorage(), new AtomicLong());
        try (storage) {
            storage.migrate();
            DefinitionRegistry registry = new DefinitionRegistry(storage);
            WorkflowEngine engine = new WorkflowEngine(storage, registry, 30_000);
            FlowSpec bp = linear("am-refuse", ExecutionMode.LOCAL_ASYNC);
            registry.register(bp.definition());
            engine.start(bp.name(), bp.version(), Map.of(), null);
            engine.start(bp.name(), bp.version(), Map.of(), null);
            List<TaskActivation> claimed = engine.poll("w1", bp.definition().queues(), 10, null);
            TaskActivation a = claimed.get(0);
            TaskActivation b = claimed.get(1);

            long before = storage.transactions.get();
            Map<String, RunResult> results = engine.advanceMany(List.of(
                    fullRun(bp, a, "w1"), fullRun(bp, b, "intruder")));
            assertEquals(1, storage.transactions.get() - before,
                    "validate refuses without burning the batch: one transaction, no replay");

            assertEquals("COMPLETED", results.get(a.taskId()).outcome().instanceStatus());
            RunResult refused = results.get(b.taskId());
            assertFalse(refused.ok());
            assertEquals(409, refused.errorStatus());

            AdvanceOutcome retry = engine.advance(b.taskId(), "w1", fullRun(bp, b, "w1").steps(), true);
            assertEquals("COMPLETED", retry.instanceStatus(), "the refused run wrote nothing and lands on retry");
        }
    }

    @Test @DisplayName("two arms of one instance in a batch: the first run wins, the second is told to retry")
    void duplicateInstanceKeepsTheFirstRun() {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            DefinitionRegistry registry = new DefinitionRegistry(storage);
            WorkflowEngine engine = new WorkflowEngine(storage, registry, 30_000);
            FlowSpec bp = FlowSpec.define("am-fork", 1, Map.class, ForkSteps.class, (f, s) ->
                    Wiggle.allOf(f.thenApply(s::a), f.thenApply(s::b))
                            .combine(s::pick)
                            .execution(ExecutionMode.LOCAL_ASYNC));
            registry.register(bp.definition());
            Set<String> queues = bp.definition().queues();
            engine.start(bp.name(), bp.version(), Map.of(), null);
            List<TaskActivation> arms = engine.poll("w1", queues, 10, null);
            assertEquals(2, arms.size(), "one poll hands the worker both arms of the fork");

            Run first = new Run(arms.get(0).taskId(), "w1",
                    List.of(new StepInput(arms.get(0).nodeId(), Map.of("a", 1L), null)), true);
            Run second = new Run(arms.get(1).taskId(), "w1",
                    List.of(new StepInput(arms.get(1).nodeId(), Map.of("b", 2L), null)), true);
            Map<String, RunResult> results = engine.advanceMany(List.of(first, second));

            assertTrue(results.get(first.startTaskId()).ok());
            RunResult loser = results.get(second.startTaskId());
            assertEquals(409, loser.errorStatus());
            assertTrue(loser.error().contains("already advances instance"), loser.error());

            // The losing arm is untouched: reported singly, the join fires and the combine runs.
            engine.advance(second.startTaskId(), "w1", second.steps(), true);
            TaskActivation combine = engine.poll("w2", queues, 10, null).getFirst();
            AdvanceOutcome done = engine.advance(combine.taskId(), "w2",
                    List.of(new StepInput(combine.nodeId(), Map.of("picked", true), null)), true);
            assertEquals("COMPLETED", done.instanceStatus());
        }
    }

    @Test @DisplayName("a run whose definition is not LOCAL_ASYNC is refused and pointed at the single path")
    void foreignModeRunIsRefused() {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            DefinitionRegistry registry = new DefinitionRegistry(storage);
            WorkflowEngine engine = new WorkflowEngine(storage, registry, 30_000);
            FlowSpec async = linear("am-async", ExecutionMode.LOCAL_ASYNC);
            FlowSpec sync = linear("am-sync", ExecutionMode.LOCAL_SYNC);
            registry.register(async.definition());
            registry.register(sync.definition());
            engine.start(async.name(), async.version(), Map.of(), null);
            engine.start(sync.name(), sync.version(), Map.of(), null);
            Set<String> queues = new java.util.HashSet<>(async.definition().queues());
            queues.addAll(sync.definition().queues());
            List<TaskActivation> claimed = engine.poll("w1", queues, 10, null);
            TaskActivation asyncTask = claimed.stream().filter(t -> t.workflow().equals(async.name())).findFirst().orElseThrow();
            TaskActivation syncTask = claimed.stream().filter(t -> t.workflow().equals(sync.name())).findFirst().orElseThrow();

            Map<String, RunResult> results = engine.advanceMany(List.of(
                    fullRun(async, asyncTask, "w1"), fullRun(sync, syncTask, "w1")));

            assertEquals("COMPLETED", results.get(asyncTask.taskId()).outcome().instanceStatus());
            RunResult refused = results.get(syncTask.taskId());
            assertEquals(409, refused.errorStatus());
            assertTrue(refused.error().contains("report this run singly"), refused.error());

            AdvanceOutcome retry = engine.advance(syncTask.taskId(), "w1", fullRun(sync, syncTask, "w1").steps(), true);
            assertEquals("COMPLETED", retry.instanceStatus(), "the single-run path still takes it");
        }
    }

    /** On H2, not in-memory: the rollback this pins is the storage's, and {@code InMemoryStorage}
     *  applies writes directly -- an exception there rolls nothing back. */
    @Test @DisplayName("a mid-run mismatch rolls the whole batch back; replay commits the innocent run alone")
    void midRunMismatchReplaysPerRun() {
        try (Storage storage = new JdbcStorage("jdbc:h2:mem:am-" + System.nanoTime()
                + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1", "sa", "", 4, new H2Dialect())) {
            storage.migrate();
            DefinitionRegistry registry = new DefinitionRegistry(storage);
            WorkflowEngine engine = new WorkflowEngine(storage, registry, 30_000);
            FlowSpec bp = linear("am-replay", ExecutionMode.LOCAL_ASYNC);
            registry.register(bp.definition());
            engine.start(bp.name(), bp.version(), Map.of(), null);
            engine.start(bp.name(), bp.version(), Map.of(), null);
            List<TaskActivation> claimed = engine.poll("w1", bp.definition().queues(), 10, null);
            TaskActivation a = claimed.get(0);
            TaskActivation b = claimed.get(1);

            // b's first step matches its token, so validate passes it; the second step lies.
            Run broken = new Run(b.taskId(), "w1", List.of(
                    new StepInput(b.nodeId(), Map.of("x", 1L), null),
                    new StepInput("no-such-node", Map.of(), null)), true);
            Map<String, RunResult> results = engine.advanceMany(List.of(fullRun(bp, a, "w1"), broken));

            assertEquals("COMPLETED", results.get(a.taskId()).outcome().instanceStatus(),
                    "the innocent run committed on replay");
            RunResult rb = results.get(b.taskId());
            assertFalse(rb.ok());
            assertEquals(409, rb.errorStatus());
            assertTrue(rb.error().contains("reported step"), rb.error());

            // The broken run's own replay rolled back whole: still at the first step, lease intact.
            AdvanceOutcome retry = engine.advance(b.taskId(), "w1", fullRun(bp, b, "w1").steps(), true);
            assertEquals("COMPLETED", retry.instanceStatus());
        }
    }

    @Test @DisplayName("a loop overrun is a durable per-run result, not an abort of the batch")
    void loopOverrunStaysDurableInsideTheBatch() {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            DefinitionRegistry registry = new DefinitionRegistry(storage);
            WorkflowEngine engine = new WorkflowEngine(storage, registry, 30_000);
            FlowSpec loop = FlowSpec.define("am-loop", 1, Map.class, LoopSteps.class, (f, s) -> f
                    .repeatWhile(s::forever, 3, b -> b.thenApply(s::spin))
                    .thenApply(s::after)
                    .execution(ExecutionMode.LOCAL_ASYNC));
            FlowSpec line = linear("am-line", ExecutionMode.LOCAL_ASYNC);
            registry.register(loop.definition());
            registry.register(line.definition());
            String loopId = engine.start(loop.name(), loop.version(), Map.of(), null);
            engine.start(line.name(), line.version(), Map.of(), null);
            Set<String> queues = new java.util.HashSet<>(loop.definition().queues());
            queues.addAll(line.definition().queues());
            List<TaskActivation> claimed = engine.poll("w1", queues, 10, null);
            TaskActivation loopTask = claimed.stream().filter(t -> t.workflow().equals(loop.name())).findFirst().orElseThrow();
            TaskActivation lineTask = claimed.stream().filter(t -> t.workflow().equals(line.name())).findFirst().orElseThrow();

            String spinNode = loopTask.nodeId();
            Node pred = loop.definition().node(loop.definition().node(spinNode).next());
            assertEquals(spinNode, GraphTraversal.successor(pred, true), "the loop is a plain cycle");
            List<StepInput> steps = new ArrayList<>();
            for (long n = 1; n <= 4; n++) {                       // budget 3: the 4th true trips it
                steps.add(new StepInput(spinNode, Map.of("n", n), null));
                steps.add(new StepInput(pred.id(), null, true));
            }
            Map<String, RunResult> results = engine.advanceMany(List.of(
                    new Run(loopTask.taskId(), "w1", steps, true), fullRun(line, lineTask, "w1")));

            assertEquals("FAILED", results.get(loopTask.taskId()).outcome().instanceStatus(),
                    "an overrun is a durable result, not an abort");
            assertEquals("COMPLETED", results.get(lineTask.taskId()).outcome().instanceStatus(),
                    "and it commits beside its batch-mates");
            InstanceView failed = engine.instance(loopId).orElseThrow();
            assertEquals("FAILED", failed.status());
            assertTrue(failed.error().contains("exceeded its budget"), failed.error());
        }
    }

    @Test @DisplayName("a compensator in a batch is answered with the COMPENSATING status; the reverse pass is untouched")
    void compensatorCannotRideABatch() {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            DefinitionRegistry registry = new DefinitionRegistry(storage);
            WorkflowEngine engine = new WorkflowEngine(storage, registry, 30_000);
            FlowSpec bp = FlowSpec.define("am-saga", 1, Map.class, SagaSteps.class, (f, s) -> f
                    .execution(ExecutionMode.LOCAL_ASYNC)
                    .thenApplyCompensable(s::reserve)
                    .thenApply(s::boom));
            registry.register(bp.definition());
            Set<String> queues = bp.definition().queues();
            String id = engine.start(bp.name(), bp.version(), Map.of(), null);

            TaskActivation reserve = engine.poll("w1", queues, 10, null).getFirst();
            engine.complete(reserve.taskId(), "w1", Map.of("reserved", true));
            TaskActivation boom = engine.poll("w1", queues, 10, null).getFirst();
            engine.fail(boom.taskId(), "w1", "kaboom", false);
            assertEquals("COMPENSATING", engine.instance(id).orElseThrow().status());
            TaskActivation comp = engine.poll("w1", queues, 10, null).getFirst();

            Map<String, RunResult> results = engine.advanceMany(List.of(new Run(comp.taskId(), "w1",
                    List.of(new StepInput(comp.nodeId(), Map.of(), null)), true)));

            RunResult r = results.get(comp.taskId());
            assertTrue(r.ok());
            assertEquals("COMPENSATING", r.outcome().instanceStatus(), "answered with the status, nothing written");
            engine.complete(comp.taskId(), "w1", null);
            assertEquals("COMPENSATED", engine.instance(id).orElseThrow().status(),
                    "the reverse pass still completes through complete");
        }
    }

    @Test @DisplayName("malformed batches refuse the whole call: caller bugs, not instance state")
    void malformedBatchesRefuseTheWholeCall() {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            WorkflowEngine engine = new WorkflowEngine(storage, new DefinitionRegistry(storage), 30_000);
            assertEquals(400, assertThrows(EngineException.class,
                    () -> engine.advanceMany(List.of())).statusCode());
            Run noSteps = new Run("t1", "w1", List.of(), true);
            assertEquals(400, assertThrows(EngineException.class,
                    () -> engine.advanceMany(List.of(noSteps))).statusCode());
            Run twice = new Run("t1", "w1", List.of(new StepInput("n", null, null)), true);
            assertEquals(400, assertThrows(EngineException.class,
                    () -> engine.advanceMany(List.of(twice, twice))).statusCode());
        }
    }

    /** Counts transactions, so a test can tell "validate refused it" from "the batch replayed". */
    private record CountingStorage(Storage delegate, AtomicLong transactions) implements Storage {

        @Override public void migrate() { delegate.migrate(); }

        @Override public <R> R inTx(Function<Tx, R> work) {
            transactions.incrementAndGet();
            return delegate.inTx(work);
        }

        @Override public void close() { delegate.close(); }
    }
}
