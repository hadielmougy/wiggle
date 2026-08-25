package dev.wiggle.postgres;

import dev.wiggle.client.dsl.Blueprint;
import dev.wiggle.client.dsl.Workflow;
import dev.wiggle.core.Ids;
import dev.wiggle.core.TaskActivation;
import dev.wiggle.jdbc.JdbcStorage;
import dev.wiggle.server.engine.DefinitionRegistry;
import dev.wiggle.server.engine.WorkflowEngine;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The PostgreSQL-only claim path ({@code FOR UPDATE SKIP LOCKED} + {@code UPDATE ... RETURNING}),
 * which H2 cannot execute, so the rest of the suite never reaches it. Opt-in: set
 * {@code WIGGLE_TEST_PG_URL} to a reachable PostgreSQL, e.g. after {@code docker compose up -d postgres}:
 *
 * <pre>
 *   WIGGLE_TEST_PG_URL=jdbc:postgresql://localhost:5433/wiggle \
 *   WIGGLE_TEST_PG_USER=wiggle WIGGLE_TEST_PG_PASSWORD=wiggle \
 *     ./gradlew :tests:test --tests "dev.wiggle.postgres.PostgresClaimTest"
 * </pre>
 */
@EnabledIfEnvironmentVariable(named = "WIGGLE_TEST_PG_URL", matches = ".+")
class PostgresClaimTest {

    private static JdbcStorage storage() {
        JdbcStorage storage = new JdbcStorage(System.getenv("WIGGLE_TEST_PG_URL"),
                System.getenv("WIGGLE_TEST_PG_USER"), System.getenv("WIGGLE_TEST_PG_PASSWORD"), 4,
                new PostgresDialect());
        storage.migrate();
        return storage;
    }

    /** A unique workflow (and so a unique queue) per run keeps this isolated from other rows. */
    private static Blueprint<Map<String, Object>> uniqueWorkflow() {
        return Workflow.define("pg-claim-" + Ids.next("wf"))
                .step("work", ctx -> ctx)
                .build();
    }

    @Test @DisplayName("the SKIP LOCKED claim leases tokens with owner and expiry")
    void claimsWithLease() {
        try (JdbcStorage storage = storage()) {
            WorkflowEngine engine = new WorkflowEngine(storage, new DefinitionRegistry(storage), 30_000);
            Blueprint<Map<String, Object>> bp = uniqueWorkflow();
            engine.register(bp.definition());
            engine.start(bp.name(), bp.version(), Map.of(), null);

            List<TaskActivation> claimed = engine.poll("pg-w1", bp.definition().workerQueues(), 10, null);
            assertEquals(1, claimed.size());
            assertEquals("pg-w1", claimed.get(0).leaseOwner());
            assertTrue(claimed.get(0).leaseExpiresAt() > System.currentTimeMillis(), "lease is in the future");

            assertTrue(engine.poll("pg-w2", bp.definition().workerQueues(), 10, null).isEmpty(),
                    "a claimed token is not offered again");
        }
    }

    @Test @DisplayName("concurrent claimers never receive the same token")
    void concurrentClaimsAreExclusive() throws Exception {
        int tasks = 20;
        try (JdbcStorage storage = storage()) {
            WorkflowEngine engine = new WorkflowEngine(storage, new DefinitionRegistry(storage), 30_000);
            Blueprint<Map<String, Object>> bp = uniqueWorkflow();
            engine.register(bp.definition());
            for (int i = 0; i < tasks; i++) engine.start(bp.name(), bp.version(), Map.of(), null);

            List<TaskActivation> all = claimConcurrently(engine, bp.definition().workerQueues(), tasks);

            Set<String> distinct = new HashSet<>();
            for (TaskActivation t : all) {
                assertTrue(distinct.add(t.taskId()), "token " + t.taskId() + " was claimed twice");
            }
            assertEquals(tasks, distinct.size(), "every token claimed exactly once across both claimers");
        }
    }

    /** Two claimers racing on the same backlog, released simultaneously. */
    private static List<TaskActivation> claimConcurrently(WorkflowEngine engine, Set<String> queues, int max)
            throws Exception {
        CountDownLatch go = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            List<Future<List<TaskActivation>>> futures = new ArrayList<>();
            for (String worker : List.of("pg-race-a", "pg-race-b")) {
                futures.add(pool.submit(() -> {
                    go.await();
                    return engine.poll(worker, queues, max, null);
                }));
            }
            go.countDown();
            List<TaskActivation> all = new ArrayList<>();
            for (Future<List<TaskActivation>> f : futures) all.addAll(f.get());
            return all;
        } finally {
            pool.shutdownNow();
        }
    }
}
