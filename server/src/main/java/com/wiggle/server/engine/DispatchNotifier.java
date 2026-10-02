package com.wiggle.server.engine;

import com.wiggle.core.WorkflowVersion;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Per-node wake-on-produce for long-polling workers (in-memory dispatch, Layer 1 — see
 * {@code docs/in-memory-dispatch.md}). When tokens are parked {@code READY} on a queue, the engine
 * {@link #signal signals} how many; parked polls wake to re-run the atomic DB claim instead of
 * discovering the work on a later fallback claim.
 *
 * <p>This is a pure latency/QPS optimisation and never the source of truth. It matches producers
 * and consumers <em>on the same node</em>; work produced on another node is found by a fallback
 * claim.
 *
 * <p>Parked polls queue in one line per {@link Interest} -- the queues and versions they can claim
 * -- and a signal wakes from the front of each line that covers a signalled queue, only until the
 * woken polls' capacity covers the tokens produced. Of each line only the poll at its front runs
 * the timed fallback claim; the rest wait for a wake or their deadline, so an idle line costs one
 * claim per fallback interval however many polls are in it. Signals are also counted per queue, so
 * a signal that lands between a poll's {@link #snapshot} and its {@link #await} is not lost.
 */
final class DispatchNotifier {

    /** What a poll can claim. Polls with equal interests are interchangeable to a signal. An empty
     *  queue set claims every queue; an empty version set, every version. */
    record Interest(Set<String> queues, Set<WorkflowVersion> versions) {

        Interest {
            queues = queues == null ? Set.of() : Set.copyOf(queues);
            versions = versions == null ? Set.of() : Set.copyOf(versions);
        }

        boolean covers(String queue) {
            return queues.isEmpty() || queues.contains(queue);
        }
    }

    private static final class Waiter {
        final Condition wake;
        final int capacity;
        boolean woken;

        Waiter(Condition wake, int capacity) {
            this.wake = wake;
            this.capacity = capacity;
        }
    }

    private final ReentrantLock lock = new ReentrantLock();
    private final Map<String, Long> versions = new HashMap<>();                 // queue -> signal count
    private final Map<Interest, ArrayDeque<Waiter>> lines = new HashMap<>();   // both guarded by lock

    /** One token parked on each of {@code queues}. */
    void signal(Set<String> queues) {
        if (queues == null || queues.isEmpty()) return;
        Map<String, Integer> produced = new HashMap<>();
        for (String q : queues) produced.put(q, 1);
        signal(produced);
    }

    /** {@code produced.get(q)} tokens parked on queue {@code q}: wakes enough parked polls to claim them. */
    void signal(Map<String, Integer> produced) {
        if (produced == null || produced.isEmpty()) return;
        lock.lock();
        try {
            for (String q : produced.keySet()) versions.merge(q, 1L, Long::sum);
            for (Map.Entry<Interest, ArrayDeque<Waiter>> line : lines.entrySet()) {
                int tokens = 0;
                for (Map.Entry<String, Integer> p : produced.entrySet()) {
                    if (line.getKey().covers(p.getKey())) tokens += p.getValue();
                }
                wake(line.getValue(), tokens);
            }
        } finally {
            lock.unlock();
        }
    }

    /** Wakes one more parked poll of {@code interest}: a claim came back full, so more may be waiting. */
    void passOn(Interest interest) {
        lock.lock();
        try {
            wake(lines.get(interest), 1);
        } finally {
            lock.unlock();
        }
    }

    /** Wakes polls from the front of {@code line} until their capacity covers {@code tokens}, then
     *  nudges the new front so it starts its fallback clock. Caller holds the lock. */
    private static void wake(ArrayDeque<Waiter> line, int tokens) {
        if (line == null || tokens <= 0) return;
        int left = tokens;
        while (left > 0 && !line.isEmpty()) {
            Waiter w = line.pollFirst();
            w.woken = true;
            w.wake.signal();
            left -= w.capacity;
        }
        Waiter front = line.peekFirst();
        if (front != null) front.wake.signal();
    }

    /** The current signal count for each queue, taken before a claim so no signal is lost. */
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
     * Parks a poll that can take {@code capacity} tokens of {@code interest}. Returns true when a
     * signal woke it, or at once when one of its queues was signalled past {@code since}; false
     * when it should claim anyway -- it has been at the front of its line for
     * {@code fallbackMillis} -- or {@code deadline} has passed.
     */
    boolean await(Interest interest, int capacity, Map<String, Long> since, long fallbackMillis, long deadline) {
        lock.lock();
        try {
            if (advanced(interest.queues(), since)) return true;
            ArrayDeque<Waiter> line = lines.computeIfAbsent(interest, k -> new ArrayDeque<>());
            Waiter me = new Waiter(lock.newCondition(), Math.max(1, capacity));
            line.addLast(me);
            long frontSince = -1;
            try {
                while (!me.woken) {
                    long now = System.currentTimeMillis();
                    long wait = deadline - now;
                    if (wait <= 0) return false;
                    if (line.peekFirst() == me) {
                        if (frontSince < 0) frontSince = now;
                        long fallbackIn = frontSince + fallbackMillis - now;
                        if (fallbackIn <= 0) return false;
                        wait = Math.min(wait, fallbackIn);
                    }
                    me.wake.await(wait, TimeUnit.MILLISECONDS);
                }
                return true;
            } finally {
                if (!me.woken) {
                    boolean wasFront = line.peekFirst() == me;
                    line.remove(me);
                    if (wasFront && !line.isEmpty()) line.peekFirst().wake.signal();
                }
                if (line.isEmpty()) lines.remove(interest, line);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } finally {
            lock.unlock();
        }
    }

    /** True if any queue's signal count is now higher than it was in {@code since}. Caller holds the lock. */
    private boolean advanced(Set<String> queues, Map<String, Long> since) {
        for (String q : queues) {
            if (versions.getOrDefault(q, 0L) > since.getOrDefault(q, 0L)) return true;
        }
        return false;
    }
}
