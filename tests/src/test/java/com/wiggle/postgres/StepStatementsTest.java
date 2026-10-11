package com.wiggle.postgres;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.core.Ids;
import com.wiggle.core.TokenStatus;
import com.wiggle.jdbc.JdbcStorage;
import com.wiggle.server.engine.DefinitionRegistry;
import com.wiggle.server.engine.WorkflowEngine;
import com.wiggle.server.store.Rows.LockedTask;
import com.wiggle.tests.StepStatementSequences;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link StepStatementSequences} on a real PostgreSQL: a task is locked with its instance in one
 * joined {@code FOR UPDATE}, and a claim is one {@code UPDATE ... SKIP LOCKED} for any number of
 * tasks. Opt-in, as {@link PostgresClaimTest}:
 *
 * <pre>
 *   WIGGLE_TEST_PG_URL=jdbc:postgresql://localhost:5433/wiggle \
 *   WIGGLE_TEST_PG_USER=wiggle WIGGLE_TEST_PG_PASSWORD=wiggle \
 *     ./gradlew :tests:test --tests "com.wiggle.postgres.StepStatementsTest"
 * </pre>
 */
@EnabledIfEnvironmentVariable(named = "WIGGLE_TEST_PG_URL", matches = ".+")
class StepStatementsTest extends StepStatementSequences {

    private static final String WORKER = "count-w1";

    @Override protected JdbcStorage storage() {
        JdbcStorage storage = new JdbcStorage(TestDb.url("PG"), TestDb.user("PG"), TestDb.password("PG"), 4,
                new PostgresDialect());
        storage.migrate();
        return storage;
    }

    @Override protected List<String> lockTask() {
        return List.of("SELECT wf_instance FOR UPDATE");
    }

    @Override protected List<String> claim(int tasks) {
        return List.of("UPDATE wf_token FOR UPDATE SKIP LOCKED");
    }

    /** One step; a worker binds it by name. */
    interface OneStep {
        Map<String, Object> a(Map<String, Object> ctx);
    }

    @Test @DisplayName("a task locked after waiting on another transaction is read as that one left it")
    @SuppressWarnings({"unchecked", "rawtypes"})
    void lockTaskReadsTheTokenAsCommitted() throws Exception {
        try (JdbcStorage storage = storage()) {
            WorkflowEngine engine = new WorkflowEngine(storage, new DefinitionRegistry(storage), 30_000);
            FlowSpec flow = FlowSpec.define("pg-lock-" + Ids.next("wf"), 1, Map.class, OneStep.class,
                    (f, s) -> f.thenApply(s::a));
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
