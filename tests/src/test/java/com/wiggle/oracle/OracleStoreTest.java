package com.wiggle.oracle;

import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.core.Ids;
import com.wiggle.jdbc.JdbcStorage;
import com.wiggle.server.engine.DefinitionRegistry;
import com.wiggle.server.engine.WorkflowEngine;
import com.wiggle.server.store.Rows;
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
 * The Oracle dialect against a real Oracle Database (e.g. the {@code gvenzl/oracle-free} image).
 * This is the only place the Oracle-specific SQL actually executes: the {@code IF NOT EXISTS}-free,
 * {@code CLOB}/{@code NUMBER}-typed DDL; {@code FETCH FIRST} row limiting; the compare-and-set
 * claim; the {@code MERGE} schedule upsert; and the empty-string-is-NULL {@code join_stack}
 * handling. Opt-in: set {@code WIGGLE_TEST_ORACLE_URL}, e.g.
 *
 * <pre>
 *   WIGGLE_TEST_ORACLE_URL=jdbc:oracle:thin:@//localhost:1521/FREEPDB1 \
 *   WIGGLE_TEST_ORACLE_USER=wiggle WIGGLE_TEST_ORACLE_PASSWORD=wiggle \
 *     ./gradlew :tests:test --tests "com.wiggle.oracle.OracleStoreTest"
 * </pre>
 */
@EnabledIfEnvironmentVariable(named = "WIGGLE_TEST_ORACLE_URL", matches = ".+")
class OracleStoreTest {

    private static JdbcStorage storage() {
        JdbcStorage storage = new JdbcStorage(System.getenv("WIGGLE_TEST_ORACLE_URL"),
                System.getenv("WIGGLE_TEST_ORACLE_USER"), System.getenv("WIGGLE_TEST_ORACLE_PASSWORD"),
                8, new OracleDialect());
        storage.migrate();
        return storage;
    }

    private static Blueprint uniqueWorkflow() {
        return Workflow.define("oracle-claim-" + Ids.next("wf"))
                .step("work")
                .build();
    }

    @Test @DisplayName("migrations bootstrap the whole schema on Oracle and a workflow round-trips")
    void migrateAndRun() {
        try (JdbcStorage storage = storage()) {
            WorkflowEngine engine = new WorkflowEngine(storage, new DefinitionRegistry(storage), 30_000);
            Blueprint bp = uniqueWorkflow();
            engine.register(bp.definition());
            String id = engine.start(bp.name(), bp.version(), Map.of(), null);
            assertTrue(id != null && !id.isBlank(), "an instance id is returned");

            var claimed = storage.inTx(tx -> tx.claimTasks("w1", null, 10, System.currentTimeMillis(),
                    System.currentTimeMillis() + 30_000));
            assertEquals(1, claimed.size(), "the single ready task is claimed");
            Rows.Token t = claimed.getFirst();
            assertEquals("w1", t.leaseOwner);
            // Oracle stores '' as NULL; the store must normalise join_stack back to "".
            assertTrue(t.joinStack != null, "join_stack is never null even though Oracle stored '' as NULL");
        }
    }

    @Test @DisplayName("the MERGE schedule upsert is idempotent on Oracle")
    void scheduleUpsert() {
        try (JdbcStorage storage = storage()) {
            String wf = "oracle-sched-" + Ids.next("wf");
            Rows.Schedule s = new Rows.Schedule();
            s.id = Ids.next("sch");
            s.workflow = wf;
            s.intervalMillis = 60_000;
            s.contextJson = "{}";
            s.nextFireAt = System.currentTimeMillis() + 60_000;
            s.createdAt = System.currentTimeMillis();

            storage.inTxVoid(tx -> tx.putSchedule(s));
            s.nextFireAt += 5_000;                 // same id -> MERGE updates rather than inserts
            storage.inTxVoid(tx -> tx.putSchedule(s));

            List<Rows.Schedule> all = storage.inTx(tx -> tx.schedules()).stream()
                    .filter(x -> x.workflow.equals(wf)).toList();
            assertEquals(1, all.size(), "the upsert left exactly one row for the workflow");
            assertEquals(s.nextFireAt, all.getFirst().nextFireAt, "the second call updated next_fire_at");
        }
    }

    @Test @DisplayName("concurrent claimers never receive the same token on Oracle")
    void concurrentClaimsAreExclusive() throws Exception {
        try (JdbcStorage storage = storage()) {
            WorkflowEngine engine = new WorkflowEngine(storage, new DefinitionRegistry(storage), 30_000);
            Blueprint bp = uniqueWorkflow();
            engine.register(bp.definition());
            int tokens = 40;
            for (int i = 0; i < tokens; i++) engine.start(bp.name(), bp.version(), Map.of(), null);

            int workers = 5;
            ExecutorService pool = Executors.newFixedThreadPool(workers);
            CountDownLatch go = new CountDownLatch(1);
            // Drain until empty rather than for a fixed number of rounds: under a concurrent claim an
            // empty batch usually means peers momentarily hold the remaining rows, not that the queue
            // is drained -- so workers keep claiming (backing off briefly on an empty batch) until
            // every token is accounted for, or a generous deadline trips.
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
