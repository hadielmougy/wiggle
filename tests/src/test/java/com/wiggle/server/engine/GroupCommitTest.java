package com.wiggle.server.engine;

import com.wiggle.server.store.InMemoryStorage;
import com.wiggle.server.store.Storage;
import com.wiggle.server.store.Tx;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The batching contract: shared transactions, ack strictly after commit, and the rolled-back
 *  batch replayed one operation per transaction. */
@Timeout(30)
class GroupCommitTest {

    /** Counts transactions; can fail the next n calls; can park inside the "commit" (after the
     *  bodies ran, before inTx returns) to observe ack ordering. */
    static final class CountingStorage implements Storage {
        final AtomicInteger txs = new AtomicInteger();
        final AtomicInteger failNext = new AtomicInteger();
        volatile CountDownLatch holdCommit;

        @Override public <R> R inTx(Function<Tx, R> work) {
            txs.incrementAndGet();
            if (failNext.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                throw new IllegalStateException("injected batch failure");
            }
            R result = work.apply(null);   // the ops under test never touch the Tx
            CountDownLatch hold = holdCommit;
            if (hold != null) {
                try {
                    hold.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return result;
        }

        @Override public void migrate() {}
        @Override public void close() {}
    }

    private static GroupCommit gc(CountingStorage storage, long lingerMillis, int maxBatch) {
        return new GroupCommit(storage, new DispatchNotifier(), new ThreadLocal<>(), lingerMillis, maxBatch);
    }

    @Test @DisplayName("operations arriving within the linger share one transaction")
    void batchesWithinLinger() throws Exception {
        CountingStorage storage = new CountingStorage();
        GroupCommit gc = gc(storage, 300, 64);
        AtomicInteger ran = new AtomicInteger();

        List<CompletableFuture<Integer>> results = new ArrayList<>();
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 6; i++) {
                int n = i;
                results.add(CompletableFuture.supplyAsync(
                        () -> gc.run(tx -> { ran.incrementAndGet(); return n; }), pool));
            }
            for (int i = 0; i < 6; i++) assertEquals(i, results.get(i).get(10, TimeUnit.SECONDS));
        }
        assertEquals(6, ran.get());
        assertEquals(1, storage.txs.get(), "six operations, one shared transaction");
    }

    @Test @DisplayName("maxBatch splits the queue into more than one transaction")
    void capSplitsBatches() throws Exception {
        CountingStorage storage = new CountingStorage();
        GroupCommit gc = gc(storage, 5_000, 4);

        List<CompletableFuture<Object>> results = new ArrayList<>();
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 8; i++) {
                results.add(CompletableFuture.supplyAsync(() -> gc.run(tx -> "ok"), pool));
            }
            for (CompletableFuture<Object> r : results) assertEquals("ok", r.get(10, TimeUnit.SECONDS));
        }
        assertEquals(2, storage.txs.get(), "eight operations under a cap of four");
    }

    @Test @DisplayName("the ack happens only after the batch commits")
    void ackOnlyAfterCommit() throws Exception {
        CountingStorage storage = new CountingStorage();
        CountDownLatch commit = new CountDownLatch(1);
        CountDownLatch bodyRan = new CountDownLatch(1);
        storage.holdCommit = commit;
        GroupCommit gc = gc(storage, 1, 4);

        AtomicBoolean acked = new AtomicBoolean();
        Thread caller = new Thread(() -> { gc.run(tx -> { bodyRan.countDown(); return null; }); acked.set(true); });
        caller.start();

        assertTrue(bodyRan.await(10, TimeUnit.SECONDS), "the body ran inside the transaction");
        Thread.sleep(100);                           // the tx is held open past the body
        assertFalse(acked.get(), "not acked while the transaction is still open");
        commit.countDown();
        caller.join(10_000);
        assertTrue(acked.get(), "acked once the transaction committed");
    }

    @Test @DisplayName("a failing operation aborts the batch; the replay isolates it")
    void failedBatchReplaysIndividually() throws Exception {
        CountingStorage storage = new CountingStorage();
        GroupCommit gc = gc(storage, 500, 64);
        AtomicInteger okRuns = new AtomicInteger();

        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            // Staggered submission pins the batch order: ok1, boom, ok2.
            CompletableFuture<Object> ok1 = CompletableFuture.supplyAsync(
                    () -> gc.run(tx -> { okRuns.incrementAndGet(); return "ok1"; }), pool);
            Thread.sleep(60);
            CompletableFuture<Object> boom = CompletableFuture.supplyAsync(
                    () -> gc.run(tx -> { throw EngineException.conflict("expected"); }), pool);
            Thread.sleep(60);
            CompletableFuture<Object> ok2 = CompletableFuture.supplyAsync(
                    () -> gc.run(tx -> { okRuns.incrementAndGet(); return "ok2"; }), pool);

            assertEquals("ok1", ok1.get(10, TimeUnit.SECONDS));
            assertEquals("ok2", ok2.get(10, TimeUnit.SECONDS));
            EngineException e = (EngineException) assertThrows(Exception.class,
                    () -> boom.get(10, TimeUnit.SECONDS)).getCause();
            assertEquals("expected", e.getMessage());
        }
        assertEquals(1 + 3, storage.txs.get(), "one aborted batch, then one transaction per operation");
        // ok1 ran in the aborted batch and again in the replay; boom stopped the batch before ok2's
        // body, so ok2 ran only in the replay. With a real transactional store the aborted run's
        // writes are rolled back, so effects land exactly once.
        assertEquals(2 + 1, okRuns.get());
    }

    @Test @DisplayName("a transient batch failure still lands every operation via the replay")
    void transientBatchFailure() throws Exception {
        CountingStorage storage = new CountingStorage();
        GroupCommit gc = gc(storage, 300, 64);
        storage.failNext.set(1);   // the batch transaction itself dies before any body runs

        List<CompletableFuture<Object>> results = new ArrayList<>();
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 3; i++) {
                results.add(CompletableFuture.supplyAsync(() -> gc.run(tx -> "ok"), pool));
            }
            for (CompletableFuture<Object> r : results) assertEquals("ok", r.get(10, TimeUnit.SECONDS));
        }
        assertEquals(1 + 3, storage.txs.get());
    }

    @Test @DisplayName("queues marked ready during a batch are signalled to pollers after the commit")
    void notifierSignalledAfterFlush() throws Exception {
        CountingStorage storage = new CountingStorage();
        DispatchNotifier notifier = new DispatchNotifier();
        ThreadLocal<Set<String>> ready = new ThreadLocal<>();
        GroupCommit gc = new GroupCommit(storage, notifier, ready, 1, 4);

        Map<String, Long> before = notifier.snapshot(Set.of("q1"));
        gc.run(tx -> { ready.get().add("q1"); return null; });
        assertTrue(notifier.awaitChange(Set.of("q1"), before, 5_000),
                "the flush signalled q1, so a parked poller would wake");
    }

    @Test @DisplayName("the guard: in-memory storage cannot host group commit")
    void inMemoryIsNotTransactional() {
        assertFalse(new InMemoryStorage().transactional(),
                "no rollback -> the replay fallback would double-apply");
        assertTrue(new CountingStorage().transactional(), "the SPI default is transactional");
    }
}
