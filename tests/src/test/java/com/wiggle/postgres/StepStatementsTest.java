package com.wiggle.postgres;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.core.Ids;
import com.wiggle.core.TaskActivation;
import com.wiggle.core.TokenStatus;
import com.wiggle.jdbc.JdbcStorage;
import com.wiggle.server.engine.DefinitionRegistry;
import com.wiggle.server.engine.WorkflowEngine;
import com.wiggle.server.engine.WorkflowEngine.Run;
import com.wiggle.server.engine.WorkflowEngine.StepInput;
import com.wiggle.server.store.Rows.LockedTask;
import com.wiggle.tests.TestDb;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What one workflow step costs the database, in round trips: every statement and commit the engine
 * issues to start instances, claim their tasks and take their reports, counted by
 * {@link CountingDriver} on a real PostgreSQL and printed per step, by statement. Measured twice:
 * on the node that registered the definition, and on one that did not, which reads the graph from
 * the store. Opt-in, as {@link PostgresClaimTest}:
 *
 * <pre>
 *   WIGGLE_TEST_PG_URL=jdbc:postgresql://localhost:5433/wiggle \
 *   WIGGLE_TEST_PG_USER=wiggle WIGGLE_TEST_PG_PASSWORD=wiggle \
 *     ./gradlew :tests:test --tests "com.wiggle.postgres.StepStatementsTest" -i
 * </pre>
 */
@EnabledIfEnvironmentVariable(named = "WIGGLE_TEST_PG_URL", matches = ".+")
class StepStatementsTest {

    private static final int INSTANCES = 50;
    private static final String WORKER = "count-w1";

    /** Three steps in a line; a worker binds them by name. */
    interface ThreeSteps {
        Map<String, Object> a(Map<String, Object> ctx);
        Map<String, Object> b(Map<String, Object> ctx);
        Map<String, Object> c(Map<String, Object> ctx);
    }

    private static JdbcStorage storage() {
        JdbcStorage storage = new JdbcStorage(CountingDriver.wrap(TestDb.url("PG")),
                TestDb.user("PG"), TestDb.password("PG"), 4, new PostgresDialect());
        storage.migrate();
        return storage;
    }

    private static FlowSpec linear() {
        return FlowSpec.define("pg-count-" + Ids.next("wf"), 1, Map.class, ThreeSteps.class,
                (f, s) -> f.thenApply(s::a).thenApply(s::b).thenApply(s::c));
    }

    @Test @DisplayName("statements and commits per step: start, claim and report, on both kinds of node")
    void countsPerStep() {
        try (JdbcStorage storage = storage()) {
            WorkflowEngine registering = new WorkflowEngine(storage, new DefinitionRegistry(storage), 30_000);
            WorkflowEngine other = new WorkflowEngine(storage, new DefinitionRegistry(storage), 30_000);
            for (WorkflowEngine engine : List.of(registering, other)) {
                FlowSpec flow = linear();
                registering.register(flow.definition());
                String node = engine == registering ? "registering node" : "other node";

                CountingDriver.reset();
                for (int i = 0; i < INSTANCES; i++) engine.start(flow.name(), flow.version(), Map.of(), null);
                print(node + ": start, per instance", CountingDriver.snapshot(), INSTANCES);

                Map<String, Long> claims = new java.util.TreeMap<>();
                Map<String, Long> reports = new java.util.TreeMap<>();
                int steps = 0;
                for (List<TaskActivation> batch = claim(engine, flow, claims); !batch.isEmpty();
                     batch = claim(engine, flow, claims)) {
                    CountingDriver.reset();
                    for (TaskActivation task : batch) {
                        engine.report(new Run(task.taskId(), WORKER,
                                List.of(new StepInput(task.nodeId(), Map.of(), null)), false));
                    }
                    CountingDriver.snapshot().forEach((k, v) -> reports.merge(k, v, Long::sum));
                    steps += batch.size();
                }
                assertEquals(3 * INSTANCES, steps, "every step of every instance was claimed and reported");
                print(node + ": claim, per step", claims, steps);
                print(node + ": report, per step", reports, steps);

                long lockReads = reports.entrySet().stream()
                        .filter(e -> e.getKey().contains("FOR UPDATE")).mapToLong(Map.Entry::getValue).sum();
                assertEquals(steps, lockReads, "a report locks its task with one statement");
                assertFalse(reports.containsKey("SELECT * FROM wf_token WHERE id=?"),
                        "the task is read by the lock, not again on its own");
            }
        }
    }

    private static List<TaskActivation> claim(WorkflowEngine engine, FlowSpec flow, Map<String, Long> into) {
        CountingDriver.reset();
        List<TaskActivation> batch = engine.poll(WORKER, flow.definition().workerQueues(), INSTANCES, null);
        CountingDriver.snapshot().forEach((k, v) -> into.merge(k, v, Long::sum));
        return batch;
    }

    private static void print(String title, Map<String, Long> counts, int per) {
        StringBuilder out = new StringBuilder("\n== ").append(title).append(" (n=").append(per).append(")\n");
        long total = 0;
        for (Map.Entry<String, Long> e : counts.entrySet()) {
            out.append(String.format("  %6.2f  %s%n", (double) e.getValue() / per, e.getKey()));
            if (!e.getKey().equals(CountingDriver.COMMIT)) total += e.getValue();
        }
        out.append(String.format("  %6.2f  statements, %.2f commits%n", (double) total / per,
                (double) counts.getOrDefault(CountingDriver.COMMIT, 0L) / per));
        System.out.print(out);
    }

    @Test @DisplayName("a task locked after waiting on another transaction is read as that one left it")
    void lockTaskReadsTheTokenAsCommitted() throws Exception {
        try (JdbcStorage storage = storage()) {
            WorkflowEngine engine = new WorkflowEngine(storage, new DefinitionRegistry(storage), 30_000);
            FlowSpec flow = linear();
            engine.register(flow.definition());
            engine.start(flow.name(), flow.version(), Map.of(), null);
            String taskId = engine.poll(WORKER, flow.definition().workerQueues(), 1, null).get(0).taskId();

            CountDownLatch holding = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(2);
            try {
                Future<?> first = pool.submit(() -> storage.inTxVoid(tx -> {
                    LockedTask locked = tx.lockTask(taskId).orElseThrow();
                    locked.token().status = TokenStatus.DONE;
                    tx.updateToken(locked.token());
                    tx.updateInstance(locked.inst());
                    holding.countDown();
                    sleep(500);
                }));
                assertTrue(holding.await(10, TimeUnit.SECONDS));
                Future<LockedTask> second = pool.submit(() -> storage.inTx(tx -> tx.lockTask(taskId).orElseThrow()));
                first.get(10, TimeUnit.SECONDS);
                LockedTask seen = second.get(10, TimeUnit.SECONDS);

                assertEquals(TokenStatus.DONE, seen.token().status, "the token as the first transaction committed it");
                LockedTask now = storage.inTx(tx -> tx.lockTask(taskId).orElseThrow());
                assertEquals(now.inst().revision, seen.inst().revision, "the instance as committed too");
            } finally {
                pool.shutdownNow();
            }
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
