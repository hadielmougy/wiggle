package com.wiggle.server.engine;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Per-node wake-on-produce for long-polling workers (in-memory dispatch, Layer 1 — see
 * {@code docs/in-memory-dispatch.md}). When a token is parked {@code READY} on a queue, the engine
 * {@link #signal signals} that queue; a poller {@link #awaitChange waiting} on that queue wakes
 * immediately instead of discovering the work on its next fallback poll.
 *
 * <p>This is a pure latency/QPS optimization and never the source of truth: it only wakes a poller so
 * it re-runs the atomic DB claim. It matches producers and consumers <em>on the same node</em>; a
 * consumer on another node still catches the work via the caller's fallback poll. Signals are
 * per-queue versioned so a waiter is woken only when one of <em>its</em> queues advances, and a signal
 * that races ahead of an {@code await} is not lost (the waiter snapshots versions before claiming and
 * compares against them).
 */
final class DispatchNotifier {

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private final Map<String, Long> versions = new HashMap<>();   // queue -> signal count, guarded by lock

    /** Records that work was parked on each of {@code queues} and wakes any waiter interested in one. */
    void signal(Set<String> queues) {
        if (queues == null || queues.isEmpty()) return;
        lock.lock();
        try {
            for (String q : queues) versions.merge(q, 1L, Long::sum);
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /** A snapshot of the current signal count for each queue, taken before a claim so no signal is lost. */
    Map<String, Long> snapshot(Set<String> queues) {
        Map<String, Long> s = new HashMap<>();
        if (queues == null || queues.isEmpty()) return s;
        lock.lock();
        try {
            for (String q : queues) s.put(q, versions.getOrDefault(q, 0L));
        } finally {
            lock.unlock();
        }
        return s;
    }

    /**
     * Blocks until one of {@code queues} is signaled past its count in {@code since}, or the timeout
     * elapses — whichever comes first. Returns early on a relevant signal; otherwise waits out the
     * timeout (the caller's fallback-poll budget).
     */
    void awaitChange(Set<String> queues, Map<String, Long> since, long timeoutMillis) {
        if (timeoutMillis <= 0) return;
        long deadline = System.currentTimeMillis() + timeoutMillis;
        lock.lock();
        try {
            while (!advanced(queues, since)) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) return;
                changed.await(remaining, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            lock.unlock();
        }
    }

    /** True if any queue's signal count is now higher than it was in {@code since}. Caller holds the lock. */
    private boolean advanced(Set<String> queues, Map<String, Long> since) {
        if (queues == null) return false;
        for (String q : queues) {
            if (versions.getOrDefault(q, 0L) > since.getOrDefault(q, 0L)) return true;
        }
        return false;
    }
}
