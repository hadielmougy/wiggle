package com.wiggle.server.engine;

import com.wiggle.core.WorkflowVersion;
import com.wiggle.core.criteria.QueueCriteria;
import com.wiggle.core.criteria.Task_;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
 * between snapshot and await is not lost; a signal for an unrelated queue does not wake it; with no
 * signal the wait bounds itself by the fallback; and a signal wakes only as many parked polls as it
 * produced tokens for, of which only the front of each line runs the fallback.
 */
class DispatchNotifierTest {

    private static final DispatchNotifier.Interest ORDERS = DispatchNotifier.Interest.byName(Set.of("orders"), null);
    private static final long LONG = 10_000;

    private static long in(long millis) {
        return System.currentTimeMillis() + millis;
    }

    /** {@code count} polls parked on {@code interest}, each taking {@code capacity}; each future
     *  answers what its await returned. Parked one at a time, so they line up in submission order. */
    private static List<Future<Boolean>> park(ExecutorService pool, DispatchNotifier n,
                                              DispatchNotifier.Interest interest, int count, int capacity,
                                              long fallbackMillis) throws Exception {
        List<Future<Boolean>> parked = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Map<String, Long> since = n.snapshot(interest);
            parked.add(pool.submit(() -> n.await(interest, capacity, since, fallbackMillis, in(LONG))));
            Thread.sleep(10);
        }
        Thread.sleep(50);
        return parked;
    }

    private static int returned(List<Future<Boolean>> parked) {
        return (int) parked.stream().filter(Future::isDone).count();
    }

    @Test @DisplayName("a signal that advances a snapshotted queue makes await return immediately (no lost wakeup)")
    void signalPastSnapshotReturnsImmediately() {
        DispatchNotifier n = new DispatchNotifier();
        Map<String, Long> since = n.snapshot(ORDERS.names());   // snapshot BEFORE the signal, as poll() does
        n.signal(Set.of("orders"));                               // work parked between snapshot and await

        long start = System.currentTimeMillis();
        assertTrue(n.await(ORDERS, 1, since, 5_000, in(5_000)), "reported a signal, not a timeout");
        assertTrue(System.currentTimeMillis() - start < 1_000, "returned promptly, did not block on the timeout");
    }

    @Test @DisplayName("a concurrent signal wakes a blocked waiter well before its timeout")
    void concurrentSignalWakesWaiter() throws Exception {
        DispatchNotifier n = new DispatchNotifier();
        Map<String, Long> since = n.snapshot(ORDERS.names());

        AtomicBoolean returned = new AtomicBoolean(false);
        Thread waiter = new Thread(() -> {
            n.await(ORDERS, 1, since, LONG, in(LONG));
            returned.set(true);
        });
        waiter.start();
        Thread.sleep(50);
        n.signal(Set.of("orders"));
        waiter.join(5_000);

        assertTrue(returned.get(), "the waiter woke on the signal, not on the 10s timeout");
    }

    @Test @DisplayName("a signal for an unrelated queue does not wake the waiter")
    void unrelatedSignalDoesNotWake() {
        DispatchNotifier n = new DispatchNotifier();
        Map<String, Long> since = n.snapshot(ORDERS.names());
        n.signal(Set.of("payments"));

        long start = System.currentTimeMillis();
        assertFalse(n.await(ORDERS, 1, since, 150, in(LONG)), "reported a fallback, not a signal");
        assertTrue(System.currentTimeMillis() - start >= 120, "waited the fallback rather than returning early");
    }

    @Test @DisplayName("with no signal at all, the front of the line returns after its fallback")
    void noSignalFallsBack() {
        DispatchNotifier n = new DispatchNotifier();
        Map<String, Long> since = n.snapshot(ORDERS.names());

        long start = System.currentTimeMillis();
        assertFalse(n.await(ORDERS, 1, since, 150, in(LONG)), "fell back, no signal");
        assertTrue(System.currentTimeMillis() - start >= 120, "waited ~the fallback");
    }

    @Test @DisplayName("the deadline ends a wait that is not at the front of its line")
    void deadlineBoundsEveryWait() throws Exception {
        DispatchNotifier n = new DispatchNotifier();
        ExecutorService pool = Executors.newCachedThreadPool();
        try {
            park(pool, n, ORDERS, 1, 1, LONG);   // holds the front, with a long fallback
            Map<String, Long> since = n.snapshot(ORDERS.names());
            long start = System.currentTimeMillis();
            assertFalse(n.await(ORDERS, 1, since, 50, in(150)));
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
            n.signal(Map.of("orders", 1));

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
            n.signal(Map.of("orders", 9));   // 9 tokens, 4 per poll: three polls

            for (int i = 0; i < 3; i++) assertTrue(parked.get(i).get(2, TimeUnit.SECONDS), "poll " + i + " woke");
            Thread.sleep(100);
            assertEquals(3, returned(parked), "only three were needed");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test @DisplayName("each line covering the queue gets its own wake")
    void eachInterestIsWoken() throws Exception {
        DispatchNotifier n = new DispatchNotifier();
        DispatchNotifier.Interest v1 = DispatchNotifier.Interest.byName(Set.of("orders"), Set.of(new WorkflowVersion("w", 1)));
        DispatchNotifier.Interest v2 = DispatchNotifier.Interest.byName(Set.of("orders"), Set.of(new WorkflowVersion("w", 2)));
        ExecutorService pool = Executors.newCachedThreadPool();
        try {
            List<Future<Boolean>> first = park(pool, n, v1, 2, 1, LONG);
            List<Future<Boolean>> second = park(pool, n, v2, 2, 1, LONG);
            n.signal(Map.of("orders", 1));

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
        DispatchNotifier.Interest vip = new DispatchNotifier.Interest(Set.of(
                QueueCriteria.from("orders").where(cb -> cb.like(Task_.CORRELATION_ID, "vip-%"))), null);
        DispatchNotifier.Interest rest = new DispatchNotifier.Interest(Set.of(
                QueueCriteria.from("orders").where(cb -> cb.not(cb.like(Task_.CORRELATION_ID, "vip-%")))), null);
        ExecutorService pool = Executors.newCachedThreadPool();
        try {
            List<Future<Boolean>> first = park(pool, n, vip, 1, 1, LONG);
            List<Future<Boolean>> second = park(pool, n, rest, 1, 1, LONG);
            n.signal(Map.of("orders", 1));

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
        DispatchNotifier.Interest scoped = DispatchNotifier.Interest.byName(Set.of("orders"), Set.of(v2));
        ExecutorService pool = Executors.newCachedThreadPool();
        try {
            List<Future<Boolean>> parked = park(pool, n, scoped, 1, 1, LONG);
            n.signalReady(Map.of(new DispatchNotifier.Ready("orders", v1), 5));
            Thread.sleep(100);
            assertEquals(0, returned(parked), "v1 work leaves the v2 poll parked");
            n.signalReady(Map.of(new DispatchNotifier.Ready("orders", v2), 1));
            assertTrue(parked.getFirst().get(2, TimeUnit.SECONDS), "v2 work wakes it");

            parked = park(pool, n, scoped, 1, 1, LONG);
            n.signal(Map.of("orders", 1));
            assertTrue(parked.getFirst().get(2, TimeUnit.SECONDS), "work of unknown version wakes it too");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test @DisplayName("#293: another version's signal between snapshot and await does not cut a scoped wait short")
    void scopedSnapshotIgnoresOtherVersions() {
        DispatchNotifier n = new DispatchNotifier();
        DispatchNotifier.Interest scoped = DispatchNotifier.Interest.byName(Set.of("orders"), Set.of(new WorkflowVersion("w", 2)));
        Map<String, Long> since = n.snapshot(scoped);
        n.signalReady(Map.of(new DispatchNotifier.Ready("orders", new WorkflowVersion("w", 1)), 1));
        long start = System.currentTimeMillis();
        assertFalse(n.await(scoped, 1, since, 150, in(LONG)), "it waited out its fallback");
        assertTrue(System.currentTimeMillis() - start >= 140, "and was not returned early");

        Map<String, Long> again = n.snapshot(scoped);
        n.signalReady(Map.of(new DispatchNotifier.Ready("orders", new WorkflowVersion("w", 2)), 1));
        assertTrue(n.await(scoped, 1, again, 5_000, in(LONG)), "its own version's signal is not lost");
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
            n.passOn(ORDERS);
            assertTrue(parked.get(0).get(2, TimeUnit.SECONDS));
            Thread.sleep(100);
            assertEquals(1, returned(parked));
        } finally {
            pool.shutdownNow();
        }
    }
}
