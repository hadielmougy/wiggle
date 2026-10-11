package com.wiggle.server.engine;

import com.wiggle.core.WorkflowVersion;
import com.wiggle.core.criteria.QueueCriteria;
import com.wiggle.server.store.ClaimFilter;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
 *
 * <p>A signal names the version of each token it announces, where it is known. A version-scoped line
 * is woken, and counts as signalled, only by its own versions or by a signal of unknown version;
 * a line for every version is woken by any.
 */
final class DispatchNotifier {

    /** What a poll can claim: its queues, each with its criteria, and its versions. Polls with equal
     *  interests are interchangeable to a signal. No queues claims every queue; no versions, every
     *  version. Polls with different criteria wait in different lines, so a signal for a task one
     *  poll's criteria reject still reaches a poll whose criteria accept it. */
    record Interest(Set<QueueCriteria> queues, Set<WorkflowVersion> versions) {

        Interest {
            queues = queues == null ? Set.of() : Set.copyOf(queues);
            versions = versions == null ? Set.of() : Set.copyOf(versions);
        }

        static Interest of(ClaimFilter filter) {
            return new Interest(Set.copyOf(filter.queues()), filter.versions());
        }

        /** A poll by queue names. */
        static Interest byName(Set<String> names, Set<WorkflowVersion> versions) {
            return of(ClaimFilter.byName(names, versions));
        }

        Set<String> names() {
            Set<String> out = new HashSet<>();
            for (QueueCriteria c : queues) out.add(c.queue());
            return out;
        }

        boolean covers(String queue) {
            return queues.isEmpty() || queues.stream().anyMatch(c -> c.queue().equals(queue));
        }

        boolean covers(Ready ready) {
            return covers(ready.queue())
                    && (versions.isEmpty() || ready.version() == null || versions.contains(ready.version()));
        }

        /** The signal counters a poll of this interest watches: its queues, or for a version-scoped
         *  poll, its versions on its queues and the unknown-version count of each. */
        List<String> counters() {
            if (versions.isEmpty()) return List.copyOf(names());
            List<String> keys = new ArrayList<>();
            for (String q : names()) {
                keys.add(versionCounter(q, null));
                for (WorkflowVersion v : versions) keys.add(versionCounter(q, v));
            }
            return keys;
        }
    }

    /** Tokens made ready on {@code queue}, of {@code version} (null when not known). */
    record Ready(String queue, WorkflowVersion version) { }

    private static String versionCounter(String queue, WorkflowVersion version) {
        return queue + '\u0000' + (version == null ? "?" : version.key());
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
    private final Map<String, Long> versions = new HashMap<>();                 // counter -> signal count
    private final Map<Interest, ArrayDeque<Waiter>> lines = new HashMap<>();   // both guarded by lock

    /** One token parked on each of {@code queues}. */
    void signal(Set<String> queues) {
        if (queues == null || queues.isEmpty()) return;
        Map<String, Integer> produced = new HashMap<>();
        for (String q : queues) produced.put(q, 1);
        signal(produced);
    }

    /** {@code produced.get(q)} tokens of unknown version parked on queue {@code q}. */
    void signal(Map<String, Integer> produced) {
        if (produced == null || produced.isEmpty()) return;
        Map<Ready, Integer> ready = new HashMap<>();
        produced.forEach((q, n) -> ready.put(new Ready(q, null), n));
        signalReady(ready);
    }

    /** {@code produced.get(r)} tokens parked as {@code r} describes: wakes enough parked polls to claim them. */
    void signalReady(Map<Ready, Integer> produced) {
        if (produced == null || produced.isEmpty()) return;
        lock.lock();
        try {
            Set<String> counted = new HashSet<>();
            for (Ready r : produced.keySet()) {
                if (counted.add(r.queue())) versions.merge(r.queue(), 1L, Long::sum);
                if (counted.add(versionCounter(r.queue(), r.version()))) {
                    versions.merge(versionCounter(r.queue(), r.version()), 1L, Long::sum);
                }
            }
            for (Map.Entry<Interest, ArrayDeque<Waiter>> line : lines.entrySet()) {
                int tokens = 0;
                for (Map.Entry<Ready, Integer> p : produced.entrySet()) {
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
        return snapshot(queues == null ? List.of() : List.copyOf(queues));
    }

    /** The current count of every signal {@code interest} watches, taken before a claim. */
    Map<String, Long> snapshot(Interest interest) {
        return snapshot(interest.counters());
    }

    private Map<String, Long> snapshot(List<String> counters) {
        Map<String, Long> s = new HashMap<>();
        if (counters.isEmpty()) return s;
        lock.lock();
        try {
            for (String c : counters) s.put(c, versions.getOrDefault(c, 0L));
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
            if (advanced(interest.counters(), since)) return true;
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

    /** True if any watched count is now higher than it was in {@code since}. Caller holds the lock. */
    private boolean advanced(List<String> counters, Map<String, Long> since) {
        for (String q : counters) {
            if (versions.getOrDefault(q, 0L) > since.getOrDefault(q, 0L)) return true;
        }
        return false;
    }
}
