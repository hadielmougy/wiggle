package dev.wiggle.sqlserver;

import dev.wiggle.client.dsl.Blueprint;
import dev.wiggle.client.dsl.Workflow;
import dev.wiggle.core.Ids;
import dev.wiggle.jdbc.JdbcStorage;
import dev.wiggle.server.engine.DefinitionRegistry;
import dev.wiggle.server.engine.WorkflowEngine;
import dev.wiggle.server.store.Rows;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The SQL Server dialect against a real Microsoft SQL Server (e.g. the
 * {@code mcr.microsoft.com/mssql/server} image): schema migration (the {@code IF NOT EXISTS}-free,
 * {@code VARCHAR(MAX)}-typed DDL and the {@code sp_getapplock} migration lock), the
 * {@code OFFSET/FETCH} row limiting, the {@code WITH (UPDLOCK, ROWLOCK)} pessimistic read behind
 * the compare-and-set claim, and the {@code MERGE} schedule upsert. Opt-in: set
 * {@code WIGGLE_TEST_SQLSERVER_URL}, e.g.
 *
 * <pre>
 *   WIGGLE_TEST_SQLSERVER_URL="jdbc:sqlserver://localhost:1433;databaseName=wiggle;encrypt=false" \
 *   WIGGLE_TEST_SQLSERVER_USER=sa WIGGLE_TEST_SQLSERVER_PASSWORD='Wiggle!Passw0rd' \
 *     ./gradlew :tests:test --tests "dev.wiggle.sqlserver.SqlServerStoreTest"
 * </pre>
 */
@EnabledIfEnvironmentVariable(named = "WIGGLE_TEST_SQLSERVER_URL", matches = ".+")
class SqlServerStoreTest {

    private static JdbcStorage storage() {
        JdbcStorage storage = new JdbcStorage(System.getenv("WIGGLE_TEST_SQLSERVER_URL"),
                System.getenv("WIGGLE_TEST_SQLSERVER_USER"), System.getenv("WIGGLE_TEST_SQLSERVER_PASSWORD"),
                8, new SqlServerDialect());
        storage.migrate();
        return storage;
    }

    private static Blueprint<Map<String, Object>> uniqueWorkflow() {
        return Workflow.define("sqlserver-claim-" + Ids.next("wf"))
                .step("work", ctx -> ctx)
                .build();
    }

    @Test @DisplayName("migrations bootstrap the whole schema on SQL Server and a workflow round-trips")
    void migrateAndRun() {
        try (JdbcStorage storage = storage()) {
            WorkflowEngine engine = new WorkflowEngine(storage, new DefinitionRegistry(storage), 30_000);
            Blueprint<Map<String, Object>> bp = uniqueWorkflow();
            engine.register(bp.definition());
            String id = engine.start(bp.name(), bp.version(), Map.of(), null);
            assertTrue(id != null && !id.isBlank(), "an instance id is returned");

            var claimed = storage.inTx(tx -> tx.claimTasks("w1", null, 10, System.currentTimeMillis(),
                    System.currentTimeMillis() + 30_000));
            assertEquals(1, claimed.size(), "the single ready task is claimed");
            assertEquals("w1", claimed.getFirst().leaseOwner);
        }
    }

    @Test @DisplayName("the MERGE schedule upsert is idempotent on SQL Server")
    void scheduleUpsert() {
        try (JdbcStorage storage = storage()) {
            String wf = "sqlserver-sched-" + Ids.next("wf");
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

    @Test @DisplayName("concurrent claimers never receive the same token on SQL Server")
    void concurrentClaimsAreExclusive() throws Exception {
        try (JdbcStorage storage = storage()) {
            WorkflowEngine engine = new WorkflowEngine(storage, new DefinitionRegistry(storage), 30_000);
            Blueprint<Map<String, Object>> bp = uniqueWorkflow();
            engine.register(bp.definition());
            int tokens = 40;
            for (int i = 0; i < tokens; i++) engine.start(bp.name(), bp.version(), Map.of(), null);

            int workers = 5;
            ExecutorService pool = Executors.newFixedThreadPool(workers);
            CountDownLatch go = new CountDownLatch(1);
            List<Future<List<String>>> futures = new java.util.ArrayList<>();
            for (int w = 0; w < workers; w++) {
                String workerId = "w" + w;
                futures.add(pool.submit(() -> {
                    go.await();
                    List<String> mine = new java.util.ArrayList<>();
                    for (int r = 0; r < 20; r++) {
                        var claimed = storage.inTx(tx -> tx.claimTasks(workerId, null, 5,
                                System.currentTimeMillis(), System.currentTimeMillis() + 30_000));
                        claimed.forEach(t -> mine.add(t.id));
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
