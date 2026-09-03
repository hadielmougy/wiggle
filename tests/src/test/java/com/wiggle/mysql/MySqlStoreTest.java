package com.wiggle.mysql;

import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.core.Ids;
import com.wiggle.jdbc.JdbcStorage;
import com.wiggle.server.engine.DefinitionRegistry;
import com.wiggle.server.engine.WorkflowEngine;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The MySQL dialect against a real MySQL/MariaDB server: schema migration (which exercises the
 * {@code IF NOT EXISTS}-stripping DDL rewrites), and the two-step {@code FOR UPDATE SKIP LOCKED}
 * claim (which H2 cannot run). Opt-in: set {@code WIGGLE_TEST_MYSQL_URL} to a reachable server, e.g.
 *
 * <pre>
 *   WIGGLE_TEST_MYSQL_URL=jdbc:mysql://localhost:3307/wiggle \
 *   WIGGLE_TEST_MYSQL_USER=wiggle WIGGLE_TEST_MYSQL_PASSWORD=wiggle \
 *     ./gradlew :tests:test --tests "com.wiggle.mysql.MySqlStoreTest"
 * </pre>
 */
@EnabledIfEnvironmentVariable(named = "WIGGLE_TEST_MYSQL_URL", matches = ".+")
class MySqlStoreTest {

    private static JdbcStorage storage() {
        JdbcStorage storage = new JdbcStorage(System.getenv("WIGGLE_TEST_MYSQL_URL"),
                System.getenv("WIGGLE_TEST_MYSQL_USER"), System.getenv("WIGGLE_TEST_MYSQL_PASSWORD"),
                8, new MySqlDialect());
        storage.migrate();
        return storage;
    }

    private static Blueprint uniqueWorkflow() {
        return Workflow.define("mysql-claim-" + Ids.next("wf"))
                .step("work", ctx -> ctx)
                .build();
    }

    @Test @DisplayName("migrations bootstrap the whole schema on MySQL and a workflow round-trips")
    void migrateAndRun() {
        try (JdbcStorage storage = storage()) {
            WorkflowEngine engine = new WorkflowEngine(storage, new DefinitionRegistry(storage), 30_000);
            Blueprint bp = uniqueWorkflow();
            engine.register(bp.definition());
            String id = engine.start(bp.name(), bp.version(), Map.of(), null);
            assertTrue(id != null && !id.isBlank(), "an instance id is returned");

            var claimed = storage.inTx(tx -> tx.claimTasks("w1", null, 10, System.currentTimeMillis(),
                    System.currentTimeMillis() + 30_000));
            assertEquals(1, claimed.size(), "the single ready task is claimed via SKIP LOCKED");
            assertEquals("w1", claimed.getFirst().leaseOwner);
        }
    }

    @Test @DisplayName("concurrent claimers never receive the same token on MySQL")
    void concurrentClaimsAreExclusive() throws Exception {
        try (JdbcStorage storage = storage()) {
            WorkflowEngine engine = new WorkflowEngine(storage, new DefinitionRegistry(storage), 30_000);
            Blueprint bp = uniqueWorkflow();
            engine.register(bp.definition());
            int tokens = 50;
            for (int i = 0; i < tokens; i++) engine.start(bp.name(), bp.version(), Map.of(), null);

            int workers = 6;
            ExecutorService pool = Executors.newFixedThreadPool(workers);
            CountDownLatch go = new CountDownLatch(1);
            // Drain until empty rather than for a fixed number of rounds: under SKIP LOCKED an empty
            // claim usually means peers momentarily hold the remaining rows, not that the queue is
            // drained -- so workers keep claiming (backing off briefly on an empty batch) until every
            // token is accounted for, or a generous deadline trips. This keeps the exclusivity check
            // exact without depending on how fast the burst of contention clears.
            AtomicInteger remaining = new AtomicInteger(tokens);
            long deadlineNanos = System.nanoTime() + java.time.Duration.ofSeconds(30).toNanos();
            List<Future<List<String>>> futures = new java.util.ArrayList<>();
            for (int w = 0; w < workers; w++) {
                String workerId = "w" + w;
                futures.add(pool.submit(() -> {
                    go.await();
                    List<String> mine = new java.util.ArrayList<>();
                    while (remaining.get() > 0 && System.nanoTime() < deadlineNanos) {
                        var claimed = storage.inTx(tx -> tx.claimTasks(workerId, null, 5,
                                System.currentTimeMillis(), System.currentTimeMillis() + 30_000));
                        if (claimed.isEmpty()) {
                            Thread.sleep(2);   // no rows free right now; let a peer's claim commit, then retry
                            continue;
                        }
                        claimed.forEach(t -> mine.add(t.id));
                        remaining.addAndGet(-claimed.size());
                    }
                    return mine;
                }));
            }
            go.countDown();
            Set<String> all = new HashSet<>();
            int total = 0;
            for (Future<List<String>> f : futures) {
                List<String> mine = f.get();
                total += mine.size();
                all.addAll(mine);
            }
            pool.shutdown();
            assertEquals(total, all.size(), "no token was handed to two workers");
            assertEquals(tokens, all.size(), "every ready token was claimed exactly once");
        }
    }
}
