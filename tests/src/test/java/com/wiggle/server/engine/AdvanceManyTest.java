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


    /**
     * A batch whose runs PARK -- non-final handback, continuation leased straight back -- ends
     * with writes still in the buffer: no END node, so no read ever flushed them. This is the
     * case the final flush exists for; the happy-path tests cannot see it because their END
     * node's hasActiveTokens read flushes everything as a side effect.
     */
    @Test @DisplayName("a batch of parked runs lands through the final flush, in one transaction")
    void parkedRunsLandThroughTheFinalFlush() {
        CountingStorage storage = new CountingStorage(new JdbcStorage("jdbc:h2:mem:amp-"
                + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1", "sa", "", 4, new H2Dialect()),
                new AtomicLong());
        try (storage) {
            storage.migrate();
            DefinitionRegistry registry = new DefinitionRegistry(storage);
            WorkflowEngine engine = new WorkflowEngine(storage, registry, 30_000);
            FlowSpec bp = linear("am-park", ExecutionMode.LOCAL_ASYNC);
            registry.register(bp.definition());
            for (int i = 0; i < 3; i++) engine.start(bp.name(), bp.version(), Map.of(), null);
            List<TaskActivation> claimed = engine.poll("w1", bp.definition().queues(), 10, null);

            long before = storage.transactions.get();
            Map<String, RunResult> results = engine.advanceMany(claimed.stream()
                    .map(t -> new Run(t.taskId(), "w1",
                            List.of(new StepInput(t.nodeId(), Map.of("x", 1L), null)), false))
                    .toList());
            assertEquals(1, storage.transactions.get() - before, "one transaction, no replay");

            for (TaskActivation t : claimed) {
                RunResult r = results.get(t.taskId());
                assertTrue(r.ok(), String.valueOf(r));
                assertNotNull(r.outcome().nextTaskId(), "the continuation was leased back");
                // The leased continuation is real only if its insert was flushed before commit.
                String yNode = bp.definition().node(t.nodeId()).next();
                AdvanceOutcome done = engine.advance(r.outcome().nextTaskId(), "w1",
                        List.of(new StepInput(yNode, Map.of("x", 1L, "y", 2L), null)), true);
                assertEquals("COMPLETED", done.instanceStatus());
            }
        }
    }

    /**
     * Completing a sub-workflow child locks its parent chain mid-apply, outside validate's
     * sorted lock set -- so children are refused to the single-run path, whose lock shape has
     * no cycles.
     */
    @Test @DisplayName("a sub-workflow child is refused: its completion locks the parent outside the sorted set")
    void subWorkflowChildIsRefused() {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            DefinitionRegistry registry = new DefinitionRegistry(storage);
            WorkflowEngine engine = new WorkflowEngine(storage, registry, 30_000);
            FlowSpec child = linear("am-child", ExecutionMode.LOCAL_ASYNC);
            FlowSpec parent = FlowSpec.define("am-parent", 1, Map.class, TwoSteps.class,
                    (f, s) -> f.thenSubFlow("sub", "am-child", Map.class));
            registry.register(child.definition());
            registry.register(parent.definition());
            String parentId = engine.start(parent.name(), parent.version(), Map.of(), null);

            TaskActivation task = engine.poll("w1", child.definition().queues(), 10, null).getFirst();
            Map<String, RunResult> results = engine.advanceMany(List.of(fullRun(child, task, "w1")));

            RunResult refused = results.get(task.taskId());
            assertEquals(409, refused.errorStatus());
            assertTrue(refused.error().contains("sub-workflow"), refused.error());

            AdvanceOutcome retry = engine.advance(task.taskId(), "w1", fullRun(child, task, "w1").steps(), true);
            assertEquals("COMPLETED", retry.instanceStatus(), "the single-run path still takes it");
            assertEquals("COMPLETED", engine.instance(parentId).orElseThrow().status(),
                    "and the child's completion resumed the parent");
        }
    }

    /**
     * A replay that itself blows up -- the storage dies mid-loop -- must still report every run:
     * earlier replays have already committed, and throwing would tell the caller nothing
     * happened when some of it durably did.
     */
    @Test @DisplayName("a storage failure during replay is a per-run 500, not a lost result map")
    void replayReportsEveryRunEvenWhenOneReplayBlowsUp() {
        FailingStorage storage = new FailingStorage(new JdbcStorage("jdbc:h2:mem:amf-"
                + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1", "sa", "", 4, new H2Dialect()));
        try (storage) {
            storage.migrate();
            DefinitionRegistry registry = new DefinitionRegistry(storage);
            WorkflowEngine engine = new WorkflowEngine(storage, registry, 30_000);
            FlowSpec bp = linear("am-outage", ExecutionMode.LOCAL_ASYNC);
            registry.register(bp.definition());
            for (int i = 0; i < 3; i++) engine.start(bp.name(), bp.version(), Map.of(), null);
            List<TaskActivation> claimed = engine.poll("w1", bp.definition().queues(), 10, null);
            TaskActivation a = claimed.get(0);
            TaskActivation b = claimed.get(1);
            TaskActivation c = claimed.get(2);

            // b breaks the batch mid-run; the storage then dies on c's replay: counting from
            // here, batch=+1, a's replay=+2, b's replay=+3, c's replay=+4.
            Run broken = new Run(b.taskId(), "w1", List.of(
                    new StepInput(b.nodeId(), Map.of("x", 1L), null),
                    new StepInput("no-such-node", Map.of(), null)), true);
            storage.failOnTx.set(storage.seen.get() + 4);
            Map<String, RunResult> results = engine.advanceMany(List.of(
                    fullRun(bp, a, "w1"), broken, fullRun(bp, c, "w1")));

            assertEquals(3, results.size(), "every run is answered");
            assertEquals("COMPLETED", results.get(a.taskId()).outcome().instanceStatus());
            assertEquals(409, results.get(b.taskId()).errorStatus());
            RunResult dead = results.get(c.taskId());
            assertEquals(500, dead.errorStatus());
            assertTrue(dead.error().contains("simulated"), dead.error());

            AdvanceOutcome retry = engine.advance(c.taskId(), "w1", fullRun(bp, c, "w1").steps(), true);
            assertEquals("COMPLETED", retry.instanceStatus(), "the failed replay wrote nothing");
        }
    }

    /**
     * The JDBC executeBatch path, on H2: the apply loop's buffered writes must land through the
     * bulk statements AND still be one transaction. A flush-ordering bug in {@code BufferedTx}
     * does not corrupt anything -- the store's row-count check throws, the batch rolls back, and
     * replay produces the same results one transaction per run -- so the result map cannot see
     * it; the transaction count is the only witness.
     */
    @Test @DisplayName("on JDBC the batch lands through executeBatch: correct, and still one transaction")
    void jdbcBatchIsOneTransaction() {
        CountingStorage storage = new CountingStorage(new JdbcStorage("jdbc:h2:mem:amb-"
                + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1", "sa", "", 4, new H2Dialect()),
                new AtomicLong());
        try (storage) {
            storage.migrate();
            DefinitionRegistry registry = new DefinitionRegistry(storage);
            WorkflowEngine engine = new WorkflowEngine(storage, registry, 30_000);
            FlowSpec bp = linear("am-jdbc", ExecutionMode.LOCAL_ASYNC);
            registry.register(bp.definition());
            for (int i = 0; i < 3; i++) engine.start(bp.name(), bp.version(), Map.of(), null);
            List<TaskActivation> claimed = engine.poll("w1", bp.definition().queues(), 10, null);
            assertEquals(3, claimed.size());

            long before = storage.transactions.get();
            Map<String, RunResult> results = engine.advanceMany(
                    claimed.stream().map(t -> fullRun(bp, t, "w1")).toList());
            assertEquals(1, storage.transactions.get() - before,
                    "the whole batch is one transaction -- a replay here means a buffered write misfired");

            for (TaskActivation t : claimed) {
                assertEquals("COMPLETED", results.get(t.taskId()).outcome().instanceStatus());
                Map<String, Object> ctx = Json.asObject(engine.instance(t.instanceId()).orElseThrow().context());
                assertEquals(1L, ctx.get("x"));
                assertEquals(2L, ctx.get("y"));
            }
        }
    }



    /**
     * The one property of the bulk lock no single-threaded test can see: {@code lockInstances}
     * must take real row locks. Were FOR UPDATE missing from its IN-list statement, every
     * behavioural test would still pass -- the batch simply would not be isolated against a
     * concurrent writer. A second transaction trying to lock past it must block until timeout.
     */
    @Test @DisplayName("lockInstances takes real row locks: a second transaction cannot lock past it")
    void bulkLockActuallyLocks() throws Exception {
        try (Storage storage = new JdbcStorage("jdbc:h2:mem:aml-" + System.nanoTime()
                + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1;LOCK_TIMEOUT=250", "sa", "", 4, new H2Dialect())) {
            storage.migrate();
            DefinitionRegistry registry = new DefinitionRegistry(storage);
            WorkflowEngine engine = new WorkflowEngine(storage, registry, 30_000);
            FlowSpec bp = linear("am-lock", ExecutionMode.LOCAL_ASYNC);
            registry.register(bp.definition());
            String id = engine.start(bp.name(), bp.version(), Map.of(), null);

            var held = new java.util.concurrent.CountDownLatch(1);
            var release = new java.util.concurrent.CountDownLatch(1);
            Thread holder = new Thread(() -> storage.inTx(tx -> {
                assertEquals(1, tx.lockInstances(List.of(id)).size());
                held.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return null;
            }));
            holder.start();
            assertTrue(held.await(5, java.util.concurrent.TimeUnit.SECONDS));
            try {
                assertThrows(RuntimeException.class,
                        () -> storage.inTx(tx -> tx.lockInstance(id)),
                        "the row must be locked; a clean read-through means FOR UPDATE is gone");
            } finally {
                release.countDown();
                holder.join(5_000);
            }
        }
    }

    /** Dies with a RuntimeException on transaction number {@code failOnTx}, once. */
    private record FailingStorage(Storage delegate, AtomicLong seen, AtomicLong failOnTx) implements Storage {

        FailingStorage(Storage delegate) { this(delegate, new AtomicLong(), new AtomicLong(Long.MAX_VALUE)); }

        @Override public void migrate() { delegate.migrate(); }

        @Override public <R> R inTx(Function<Tx, R> work) {
            if (seen.incrementAndGet() == failOnTx.get()) {
                throw new IllegalStateException("simulated storage outage");
            }
            return delegate.inTx(work);
        }

        @Override public void close() { delegate.close(); }
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
