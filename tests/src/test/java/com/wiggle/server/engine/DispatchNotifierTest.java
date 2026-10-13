package com.wiggle.server.engine;

import com.wiggle.core.WorkflowVersion;
import com.wiggle.core.criteria.QueueCriteria;
import com.wiggle.core.criteria.TaskFacts;
import com.wiggle.core.criteria.Task_;
import com.wiggle.server.store.ClaimFilter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the wake-on-produce notifier (in-memory dispatch, Layer 1). These pin the contract the
 * long-poll relies on: a signal for one of a waiter's queues wakes it promptly; a signal that lands
 * while the poll is not parked is not lost; a signal for an unrelated queue does not wake it; with no
 * signal the wait bounds itself by the fallback; and a signal wakes only as many parked polls as it
 * produced tokens for, of which only the front of each line runs the fallback.
 */
class DispatchNotifierTest {

    private static final ClaimFilter ORDERS = ClaimFilter.byName(Set.of("orders"), null);
    private static final long LONG = 10_000;

    private static long in(long millis) {
        return System.currentTimeMillis() + millis;
    }

    /** {@code count} polls parked on {@code interest}, each taking {@code capacity}; each future
     *  answers what its await returned. Parked one at a time, so they line up in submission order. */
    private static List<Future<Boolean>> park(ExecutorService pool, DispatchNotifier n,
                                              ClaimFilter interest, int count, int capacity,
                                              long fallbackMillis) throws Exception {
        List<Future<Boolean>> parked = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            DispatchNotifier.Watch watch = n.watch(interest, capacity);
            parked.add(pool.submit(() -> watch.await(fallbackMillis, in(LONG))));
            Thread.sleep(10);
        }
        Thread.sleep(50);
        return parked;
    }

    /** {@code count} tokens made ready on {@code queue} in one committed transaction. */
    private static void signal(DispatchNotifier n, String queue, WorkflowVersion version, TaskFacts facts, int count) {
        DispatchNotifier.Batch batch = n.batch();
        for (int i = 0; i < count; i++) batch.ready(queue, version, facts);
        batch.signal();
    }

    private static int returned(List<Future<Boolean>> parked) {
        return (int) parked.stream().filter(Future::isDone).count();
    }

    @Test @DisplayName("a signal that lands after the watch opens makes await return immediately (no lost wakeup)")
    void signalPastSnapshotReturnsImmediately() {
        DispatchNotifier n = new DispatchNotifier();
        DispatchNotifier.Watch watch = n.watch(ORDERS, 1);   // watch BEFORE the signal, as poll() does
        signal(n, "orders", null, null, 1);   // work produced before the poll parks

        long start = System.currentTimeMillis();
        assertTrue(watch.await(5_000, in(5_000)), "reported a signal, not a timeout");
        assertTrue(System.currentTimeMillis() - start < 1_000, "returned promptly, did not block on the timeout");
    }

    @Test @DisplayName("a concurrent signal wakes a blocked waiter well before its timeout")
    void concurrentSignalWakesWaiter() throws Exception {
        DispatchNotifier n = new DispatchNotifier();
        DispatchNotifier.Watch watch = n.watch(ORDERS, 1);

        AtomicBoolean returned = new AtomicBoolean(false);
        Thread waiter = new Thread(() -> {
            watch.await(LONG, in(LONG));
            returned.set(true);
        });
        waiter.start();
        Thread.sleep(50);
        signal(n, "orders", null, null, 1);
        waiter.join(5_000);

        assertTrue(returned.get(), "the waiter woke on the signal, not on the 10s timeout");
    }

    @Test @DisplayName("a signal for an unrelated queue does not wake the waiter")
    void unrelatedSignalDoesNotWake() {
        DispatchNotifier n = new DispatchNotifier();
        DispatchNotifier.Watch watch = n.watch(ORDERS, 1);
        signal(n, "payments", null, null, 1);

        long start = System.currentTimeMillis();
        assertFalse(watch.await(150, in(LONG)), "reported a fallback, not a signal");
        assertTrue(System.currentTimeMillis() - start >= 120, "waited the fallback rather than returning early");
    }

    @Test @DisplayName("with no signal at all, the front of the line returns after its fallback")
    void noSignalFallsBack() {
        DispatchNotifier n = new DispatchNotifier();
        DispatchNotifier.Watch watch = n.watch(ORDERS, 1);

        long start = System.currentTimeMillis();
        assertFalse(watch.await(150, in(LONG)), "fell back, no signal");
        assertTrue(System.currentTimeMillis() - start >= 120, "waited ~the fallback");
    }

    @Test @DisplayName("the deadline ends a wait that is not at the front of its line")
    void deadlineBoundsEveryWait() throws Exception {
        DispatchNotifier n = new DispatchNotifier();
        ExecutorService pool = Executors.newCachedThreadPool();
        try {
            park(pool, n, ORDERS, 1, 1, LONG);   // holds the front, with a long fallback
            DispatchNotifier.Watch watch = n.watch(ORDERS, 1);
            long start = System.currentTimeMillis();
            assertFalse(watch.await(50, in(150)));
            long waited = System.currentTimeMillis() - start;
            assertTrue(waited >= 120, "behind the front its own fallback does not run: waited " + waited);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test @DisplayName("one token wakes one of several parked polls, not all of them")
    void oneTokenWakesOnePoll() throws Exception {
        DispatchNotifier n = new DispatchNotifier();
        ExecutorService pool = Executors.newCachedThreadPool();
        try {
            List<Future<Boolean>> parked = park(pool, n, ORDERS, 6, 1, LONG);
            signal(n, "orders", null, null, 1);

            assertTrue(parked.get(0).get(2, TimeUnit.SECONDS), "the front poll woke on the signal");
            Thread.sleep(100);
            assertEquals(1, returned(parked), "the other five stayed parked");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test @DisplayName("a burst wakes polls until their capacity covers it")
    void burstWakesByCapacity() throws Exception {
        DispatchNotifier n = new DispatchNotifier();
        ExecutorService pool = Executors.newCachedThreadPool();
        try {
            List<Future<Boolean>> parked = park(pool, n, ORDERS, 6, 4, LONG);
            signal(n, "orders", null, null, 9);   // 9 tokens, 4 per poll: three polls

            for (int i = 0; i < 3; i++) assertTrue(parked.get(i).get(2, TimeUnit.SECONDS), "poll " + i + " woke");
            Thread.sleep(100);
            assertEquals(3, returned(parked), "only three were needed");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test @DisplayName("each line covering the queue gets its own wake")
    void eachFilterIsWoken() throws Exception {
        DispatchNotifier n = new DispatchNotifier();
        ClaimFilter v1 = ClaimFilter.byName(Set.of("orders"), Set.of(new WorkflowVersion("w", 1)));
        ClaimFilter v2 = ClaimFilter.byName(Set.of("orders"), Set.of(new WorkflowVersion("w", 2)));
        ExecutorService pool = Executors.newCachedThreadPool();
        try {
            List<Future<Boolean>> first = park(pool, n, v1, 2, 1, LONG);
            List<Future<Boolean>> second = park(pool, n, v2, 2, 1, LONG);
            signal(n, "orders", null, null, 1);

            assertTrue(first.get(0).get(2, TimeUnit.SECONDS), "v1's front woke");
            assertTrue(second.get(0).get(2, TimeUnit.SECONDS), "v2's front woke: it may be the one that can take it");
            Thread.sleep(100);
            assertEquals(1, returned(first));
            assertEquals(1, returned(second));
        } finally {
            pool.shutdownNow();
        }
    }

    @Test @DisplayName("WGL-QC-080: polls with different criteria wait in different lines, so each is woken")
    void eachCriteriaIsWoken() throws Exception {
        DispatchNotifier n = new DispatchNotifier();
        ClaimFilter vip = new ClaimFilter(List.of(
                QueueCriteria.from("orders").where(cb -> cb.like(Task_.CORRELATION_ID, "vip-%"))), null);
        ClaimFilter rest = new ClaimFilter(List.of(
                QueueCriteria.from("orders").where(cb -> cb.not(cb.like(Task_.CORRELATION_ID, "vip-%")))), null);
        ExecutorService pool = Executors.newCachedThreadPool();
        try {
            List<Future<Boolean>> first = park(pool, n, vip, 1, 1, LONG);
            List<Future<Boolean>> second = park(pool, n, rest, 1, 1, LONG);
            signal(n, "orders", null, null, 1);

            assertTrue(first.get(0).get(2, TimeUnit.SECONDS), "vip's poll woke");
            assertTrue(second.get(0).get(2, TimeUnit.SECONDS), "and so did rest's: only its criteria may accept the task");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test @DisplayName("#293: a version-scoped line is woken by its own versions and by unknown ones, not by others")
    void scopedLinesWakeForTheirVersions() throws Exception {
        DispatchNotifier n = new DispatchNotifier();
        WorkflowVersion v1 = new WorkflowVersion("w", 1);
        WorkflowVersion v2 = new WorkflowVersion("w", 2);
        ClaimFilter scoped = ClaimFilter.byName(Set.of("orders"), Set.of(v2));
        ExecutorService pool = Executors.newCachedThreadPool();
        try {
            List<Future<Boolean>> parked = park(pool, n, scoped, 1, 1, LONG);
            signal(n, "orders", v1, null, 5);
            Thread.sleep(100);
            assertEquals(0, returned(parked), "v1 work leaves the v2 poll parked");
            signal(n, "orders", v2, null, 1);
            assertTrue(parked.getFirst().get(2, TimeUnit.SECONDS), "v2 work wakes it");

            parked = park(pool, n, scoped, 1, 1, LONG);
            signal(n, "orders", null, null, 1);
            assertTrue(parked.getFirst().get(2, TimeUnit.SECONDS), "work of unknown version wakes it too");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test @DisplayName("#293: another version's signal while not parked does not cut a scoped wait short")
    void scopedSnapshotIgnoresOtherVersions() {
        DispatchNotifier n = new DispatchNotifier();
        ClaimFilter scoped = ClaimFilter.byName(Set.of("orders"), Set.of(new WorkflowVersion("w", 2)));
        DispatchNotifier.Watch watch = n.watch(scoped, 1);
        signal(n, "orders", new WorkflowVersion("w", 1), null, 1);
        long start = System.currentTimeMillis();
        assertFalse(watch.await(150, in(LONG)), "it waited out its fallback");
        assertTrue(System.currentTimeMillis() - start >= 140, "and was not returned early");

        signal(n, "orders", new WorkflowVersion("w", 2), null, 1);
        assertTrue(watch.await(5_000, in(LONG)), "its own version's signal is not lost");
    }

    @Test @DisplayName("only the front of a line runs the fallback; the next takes over when it leaves")
    void onlyTheFrontFallsBack() throws Exception {
        DispatchNotifier n = new DispatchNotifier();
        ExecutorService pool = Executors.newCachedThreadPool();
        try {
            List<Future<Boolean>> parked = park(pool, n, ORDERS, 5, 1, 150);   // front's fallback is under way
            assertFalse(parked.get(0).get(2, TimeUnit.SECONDS), "the front fell back");
            Thread.sleep(40);
            assertEquals(1, returned(parked), "the rest are still parked, waiting for a wake");
            assertFalse(parked.get(1).get(2, TimeUnit.SECONDS), "the next in line became the front and fell back");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test @DisplayName("passOn wakes the next parked poll of the same interest")
    void passOnWakesTheNext() throws Exception {
        DispatchNotifier n = new DispatchNotifier();
        ExecutorService pool = Executors.newCachedThreadPool();
        try {
            List<Future<Boolean>> parked = park(pool, n, ORDERS, 3, 1, LONG);
            n.watch(ORDERS, 1).passOn();
            assertTrue(parked.get(0).get(2, TimeUnit.SECONDS));
            Thread.sleep(100);
            assertEquals(1, returned(parked));
        } finally {
            pool.shutdownNow();
        }
    }

    @Test @DisplayName("a full claim no signal preceded wakes the next parked poll, once")
    void fullUnsignaledClaimHandsOn() throws Exception {
        DispatchNotifier n = new DispatchNotifier();
        ExecutorService pool = Executors.newCachedThreadPool();
        try {
            DispatchNotifier.Watch watch = n.watch(ORDERS, 1);
            List<Future<Boolean>> parked = park(pool, n, ORDERS, 3, 1, LONG);

            assertEquals("got", watch.claim(() -> "got", got -> true));
            assertTrue(parked.get(0).get(2, TimeUnit.SECONDS), "more work may wait: the next poll was woken");
            Thread.sleep(100);
            assertEquals(1, returned(parked), "one hand-off per full claim");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test @DisplayName("a full claim a signal preceded hands nothing on; the next unsignaled one does")
    void signaledClaimDoesNotHandOn() throws Exception {
        DispatchNotifier n = new DispatchNotifier();
        ExecutorService pool = Executors.newCachedThreadPool();
        try {
            DispatchNotifier.Watch watch = n.watch(ORDERS, 1);
            signal(n, "orders", null, null, 1);
            assertTrue(watch.await(LONG, in(LONG)));
            List<Future<Boolean>> parked = park(pool, n, ORDERS, 3, 1, LONG);

            watch.claim(() -> "got", got -> true);
            Thread.sleep(100);
            assertEquals(0, returned(parked), "the signal already woke as many polls as it had tokens");

            watch.claim(() -> "got", got -> true);
            assertTrue(parked.get(0).get(2, TimeUnit.SECONDS), "a claim spends the signal; the next one is unsignaled");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test @DisplayName("a poll cancelled after a signal hands the wake on, once")
    void cancelledAfterSignalHandsOn() throws Exception {
        DispatchNotifier n = new DispatchNotifier();
        ExecutorService pool = Executors.newCachedThreadPool();
        try {
            DispatchNotifier.Watch watch = n.watch(ORDERS, 1);
            signal(n, "orders", null, null, 1);
            assertTrue(watch.await(LONG, in(LONG)));
            List<Future<Boolean>> parked = park(pool, n, ORDERS, 3, 1, LONG);

            watch.forgo();
            watch.forgo();
            assertTrue(parked.get(0).get(2, TimeUnit.SECONDS), "the wake went to the next poll");
            Thread.sleep(100);
            assertEquals(1, returned(parked), "and only one");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test @DisplayName("a claim that throws propagates and hands nothing on, signaled or not")
    void failedClaimHandsNothingOn() throws Exception {
        DispatchNotifier n = new DispatchNotifier();
        ExecutorService pool = Executors.newCachedThreadPool();
        try {
            DispatchNotifier.Watch watch = n.watch(ORDERS, 1);
            List<Future<Boolean>> parked = park(pool, n, ORDERS, 3, 1, LONG);
            IllegalStateException down = new IllegalStateException("store down");

            assertSame(down, assertThrows(IllegalStateException.class,
                    () -> watch.claim(() -> { throw down; }, got -> true)));
            signal(n, "orders", null, null, 1);
            assertTrue(parked.get(0).get(2, TimeUnit.SECONDS));
            assertTrue(watch.await(LONG, in(LONG)));
            assertThrows(IllegalStateException.class, () -> watch.claim(() -> { throw down; }, got -> true));
            watch.forgo();
            Thread.sleep(100);
            assertEquals(1, returned(parked), "only the signal's own wake; the failed claims spent theirs");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test @DisplayName("only a wake that went unused is forwarded: not a fallback's, nor a short claim's")
    void forwardsOnlyAnUnusedWake() throws Exception {
        DispatchNotifier n = new DispatchNotifier();
        ExecutorService pool = Executors.newCachedThreadPool();
        try {
            DispatchNotifier.Watch watch = n.watch(ORDERS, 1);
            assertFalse(watch.await(20, in(LONG)), "fell back");
            List<Future<Boolean>> parked = park(pool, n, ORDERS, 3, 1, LONG);
            watch.forgo();
            watch.claim(() -> "short", got -> false);
            Thread.sleep(100);
            assertEquals(0, returned(parked), "no signal to forward, and a short claim found all there was");

            signal(n, "orders", null, null, 1);
            assertTrue(parked.get(0).get(2, TimeUnit.SECONDS), "the signal woke the front poll");
            assertTrue(watch.await(LONG, in(LONG)), "and this poll saw it land");
            watch.forgo();
            assertTrue(parked.get(1).get(2, TimeUnit.SECONDS), "its unused wake went to the next poll");
            Thread.sleep(100);
            assertEquals(2, returned(parked));
        } finally {
            pool.shutdownNow();
        }
    }

    private static final ClaimFilter VIP = new ClaimFilter(List.of(
            QueueCriteria.from("orders").where(cb -> cb.like(Task_.CORRELATION_ID, "vip-%"))), null);

    private static TaskFacts task(String correlationId) {
        return new TaskFacts("w", 1, "orders", "pay", "n1", "TASK", 1, correlationId, 0);
    }

    private static void signal(DispatchNotifier n, TaskFacts facts, int count) {
        signal(n, "orders", new WorkflowVersion("w", 1), facts, count);
    }

    @Test @DisplayName("a criteria line is woken by tasks its criteria admit and by unknown ones, not by others")
    void criteriaLinesWakeForMatchingTasks() throws Exception {
        DispatchNotifier n = new DispatchNotifier();
        ExecutorService pool = Executors.newCachedThreadPool();
        try {
            List<Future<Boolean>> parked = park(pool, n, VIP, 1, 1, LONG);
            signal(n, task("std-1"), 5);
            Thread.sleep(100);
            assertEquals(0, returned(parked), "std work leaves the vip poll parked");
            signal(n, task("vip-1"), 1);
            assertTrue(parked.getFirst().get(2, TimeUnit.SECONDS), "vip work wakes it");

            parked = park(pool, n, VIP, 1, 1, LONG);
            signal(n, "orders", null, null, 1);
            assertTrue(parked.getFirst().get(2, TimeUnit.SECONDS), "work whose facts are unknown wakes it too");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test @DisplayName("a task the criteria reject, while not parked, does not cut the wait short")
    void criteriaSnapshotIgnoresRejectedTasks() {
        DispatchNotifier n = new DispatchNotifier();
        DispatchNotifier.Watch watch = n.watch(VIP, 1);
        signal(n, task("std-1"), 1);
        long start = System.currentTimeMillis();
        assertFalse(watch.await(150, in(LONG)), "it waited out its fallback");
        assertTrue(System.currentTimeMillis() - start >= 140, "and was not returned early");

        signal(n, task("vip-1"), 1);
        assertTrue(watch.await(5_000, in(LONG)), "a task it admits is not lost");
    }
}
