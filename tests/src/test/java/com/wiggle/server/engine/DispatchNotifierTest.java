package com.wiggle.server.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the wake-on-produce notifier (in-memory dispatch, Layer 1). These pin the contract the
 * long-poll relies on: a signal for one of a waiter's queues wakes it promptly; a signal that lands
 * between snapshot and await is not lost; a signal for an unrelated queue does not wake it; and with no
 * signal the wait bounds itself by the fallback timeout.
 */
class DispatchNotifierTest {

    @Test @DisplayName("a signal that advances a snapshotted queue makes awaitChange return immediately (no lost wakeup)")
    void signalPastSnapshotReturnsImmediately() {
        DispatchNotifier n = new DispatchNotifier();
        Set<String> queues = Set.of("orders");
        Map<String, Long> since = n.snapshot(queues);   // snapshot BEFORE the signal, as poll() does
        n.signal(Set.of("orders"));                      // work parked between snapshot and await

        long start = System.currentTimeMillis();
        boolean signaled = n.awaitChange(queues, since, 5_000);   // must not wait: already advanced past `since`
        assertTrue(signaled, "reported a signal, not a timeout");
        assertTrue(System.currentTimeMillis() - start < 1_000, "returned promptly, did not block on the timeout");
    }

    @Test @DisplayName("a concurrent signal wakes a blocked waiter well before its timeout")
    void concurrentSignalWakesWaiter() throws Exception {
        DispatchNotifier n = new DispatchNotifier();
        Set<String> queues = Set.of("orders");
        Map<String, Long> since = n.snapshot(queues);

        AtomicBoolean returned = new AtomicBoolean(false);
        Thread waiter = new Thread(() -> {
            n.awaitChange(queues, since, 10_000);
            returned.set(true);
        });
        waiter.start();
        Thread.sleep(50);                 // let the waiter block
        n.signal(Set.of("orders"));       // produce work on the same node
        waiter.join(5_000);

        assertTrue(returned.get(), "the waiter woke on the signal, not on the 10s timeout");
        assertFalse(waiter.isAlive(), "waiter thread finished");
    }

    @Test @DisplayName("a signal for an unrelated queue does not wake the waiter")
    void unrelatedSignalDoesNotWake() {
        DispatchNotifier n = new DispatchNotifier();
        Set<String> mine = Set.of("orders");
        Map<String, Long> since = n.snapshot(mine);
        n.signal(Set.of("payments"));     // different queue

        long start = System.currentTimeMillis();
        boolean signaled = n.awaitChange(mine, since, 150);  // no relevant signal -> must wait out the timeout
        assertFalse(signaled, "reported a timeout, not a signal");
        assertTrue(System.currentTimeMillis() - start >= 120, "waited the timeout rather than returning early");
    }

    @Test @DisplayName("with no signal at all, awaitChange bounds itself by the timeout")
    void noSignalTimesOut() {
        DispatchNotifier n = new DispatchNotifier();
        Set<String> queues = Set.of("orders");
        Map<String, Long> since = n.snapshot(queues);

        long start = System.currentTimeMillis();
        boolean signaled = n.awaitChange(queues, since, 150);
        assertFalse(signaled, "timed out, no signal");
        assertTrue(System.currentTimeMillis() - start >= 120, "waited ~the timeout");
    }
}
