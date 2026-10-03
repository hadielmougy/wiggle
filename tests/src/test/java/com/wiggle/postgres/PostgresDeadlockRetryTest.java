package com.wiggle.postgres;

import com.wiggle.core.InstanceStatus;
import com.wiggle.jdbc.JdbcStorage;
import com.wiggle.server.store.Rows;
import com.wiggle.tests.TestDb;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A real recoverable failure, from a real database: two transactions take the same two instance locks
 * in opposite order, PostgreSQL detects the cycle and kills one of them with {@code 40P01}.
 *
 * <p>This is the failure the store's replay exists for, and the only way to see it is to provoke it --
 * an injected exception proves the classification but not that the driver reports what the dialect
 * claims, nor that a fresh connection actually completes the work the victim lost. Both transactions
 * MUST commit, and the victim's own body MUST have run twice.
 *
 * <p>Opt-in: set {@code WIGGLE_TEST_PG_URL} to a reachable PostgreSQL, e.g. after
 * {@code docker compose up -d postgres}:
 *
 * <pre>
 *   WIGGLE_TEST_PG_URL=jdbc:postgresql://localhost:5433/wiggle \
 *   WIGGLE_TEST_PG_USER=wiggle WIGGLE_TEST_PG_PASSWORD=wiggle \
 *     ./gradlew :tests:test --tests "com.wiggle.postgres.PostgresDeadlockRetryTest"
 * </pre>
 */
@EnabledIfEnvironmentVariable(named = "WIGGLE_TEST_PG_URL", matches = ".+")
class PostgresDeadlockRetryTest {

    private static JdbcStorage storage() {
        JdbcStorage storage = new JdbcStorage(TestDb.url("PG"),
                TestDb.user("PG"), TestDb.password("PG"), 4, new PostgresDialect());
        storage.migrate();
        return storage;
    }

    private static Rows.Instance instance(String id) {
        Rows.Instance i = new Rows.Instance();
        i.id = id;
        i.workflow = "pg-deadlock";
        i.version = 1;
        i.status = InstanceStatus.RUNNING;
        long now = System.currentTimeMillis();
        i.createdAt = now;
        i.updatedAt = now;
        return i;
    }

    @Test @DisplayName("a PostgreSQL deadlock victim is replayed, and both transactions commit")
    void deadlockVictimIsReplayed() throws Exception {
        String a = "wfi_dl_a_" + System.nanoTime();
        String b = "wfi_dl_b_" + System.nanoTime();
        AtomicInteger bodies = new AtomicInteger();
        // The two transactions pause between their locks so the cycle is certain to form. On the
        // victim's replay the latches are already open, so it runs straight through.
        CountDownLatch tookA = new CountDownLatch(1);
        CountDownLatch tookB = new CountDownLatch(1);

        try (JdbcStorage storage = storage()) {
            storage.inTxVoid(tx -> {
                tx.insertInstance(instance(a));
                tx.insertInstance(instance(b));
            });

            ExecutorService pool = Executors.newFixedThreadPool(2);
            try {
                Future<String> first = pool.submit(() -> storage.inTx(tx -> {
                    bodies.incrementAndGet();
                    tx.lockInstance(a);
                    tookA.countDown();
                    await(tookB);
                    tx.lockInstance(b);
                    return "a-then-b";
                }));
                Future<String> second = pool.submit(() -> storage.inTx(tx -> {
                    bodies.incrementAndGet();
                    tx.lockInstance(b);
                    tookB.countDown();
                    await(tookA);
                    tx.lockInstance(a);
                    return "b-then-a";
                }));

                assertEquals("a-then-b", first.get(60, TimeUnit.SECONDS), "the a-then-b transaction committed");
                assertEquals("b-then-a", second.get(60, TimeUnit.SECONDS), "the b-then-a transaction committed");
                assertTrue(bodies.get() >= 3, "one of the two was the deadlock victim and ran again; "
                        + "bodies run: " + bodies.get());
            } finally {
                pool.shutdownNow();
            }

            List<Rows.Instance> locked = storage.inTx(tx -> tx.lockInstances(List.of(a, b).stream().sorted().toList()));
            assertEquals(2, locked.size(), "both rows survived the deadlock and its replay");
        }
    }

    /** Waits for the other transaction to take its first lock; a timeout is not a failure here,
     *  only the replay running when the other side is already finished. */
    private static void await(CountDownLatch latch) {
        try {
            latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted waiting for the other transaction", e);
        }
    }
}
