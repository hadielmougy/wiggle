package com.wiggle.server.engine;

import com.wiggle.core.WorkflowVersion;
import com.wiggle.core.criteria.Criteria;
import com.wiggle.core.criteria.QueueCriteria;
import com.wiggle.core.criteria.TaskFacts;
import com.wiggle.server.store.ClaimFilter;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Per-node wake-on-produce for long-polling workers (in-memory dispatch, Layer 1). When tokens are
 * parked {@code READY}, the engine signals them through a {@link Batch}; parked polls wake to re-run the
 * atomic DB claim instead of discovering the work on a later fallback claim.
 *
 * <p>This is a pure latency/QPS optimisation and never the source of truth. It matches producers
 * and consumers <em>on the same node</em>; work produced on another node is found by a fallback
 * claim.
 *
 * <p>A poll {@link #watch watches} its claim filter for as long as it lasts. Watches of equal
 * filters share one line, and a signal wakes from the front of each line whose filter covers a
 * signalled token -- its queue, its version where known, and its facts where known -- only until
 * the woken polls' capacity covers the tokens produced. Of each line only the poll at its front runs
 * the timed fallback claim; the rest wait for a wake or their deadline, so an idle line costs one
 * claim per fallback interval however many polls are in it. Each line also counts the signals
 * that cover it, so a signal that lands while a poll is claiming rather than parked is not lost.
 */
final class DispatchNotifier {

    /** What a poll can claim, as a line key: its filter's queues, each with its criteria, and its
     *  versions, in any order. Polls with equal interests are interchangeable to a signal. Polls with
     *  different criteria wait in different lines, so a signal for a task one poll's criteria reject
     *  still reaches a poll whose criteria accept it. */
    private record Interest(Set<QueueCriteria> queues, Set<WorkflowVersion> versions) {

        static Interest of(ClaimFilter filter) {
            return new Interest(Set.copyOf(filter.queues()), filter.versions());
        }

        /** Whether a poll of this interest could claim a token {@code ready} describes. */
        boolean covers(Ready ready) {
            if (!queues.isEmpty() && queues.stream().noneMatch(c -> c.queue().equals(ready.queue()))) return false;
            if (!versions.isEmpty() && ready.version() != null && !versions.contains(ready.version())) return false;
            if (ready.facts() == null || queues.isEmpty()) return true;
            QueueCriteria c = QueueCriteria.forQueue(queues, ready.queue());
            return c.where() == null || Criteria.matches(c.where(), ready.facts());
        }
    }

    /** Tokens made ready on {@code queue}, of {@code version} and with {@code facts} (each null when
     *  not known). */
    private record Ready(String queue, WorkflowVersion version, TaskFacts facts) {}

    private static final class Waiter {
        final Condition wake;
        final int capacity;
        boolean woken;

        Waiter(Condition wake, int capacity) {
            this.wake = wake;
            this.capacity = capacity;
        }
    }

    /** One interest's state: the signals that covered it, its parked polls, and its open watches. */
    private static final class Line {
        long signals;
        final ArrayDeque<Waiter> parked = new ArrayDeque<>();
        int watches;
    }

    private final ReentrantLock lock = new ReentrantLock();
    private final Map<Interest, Line> lines = new HashMap<>();   // guarded by lock

    /** Opens a batch to collect the tokens one transaction makes ready, for signalling once it commits. */
    Batch batch() {
        return new Batch();
    }

    /** {@code produced.get(r)} tokens parked as {@code r} describes: wakes enough parked polls to claim them. */
    private void signal(Map<Ready, Integer> produced) {
        if (produced.isEmpty()) return;
        lock.lock();
        try {
            for (Map.Entry<Interest, Line> e : lines.entrySet()) {
                int tokens = 0;
                for (Map.Entry<Ready, Integer> p : produced.entrySet()) {
                    if (e.getKey().covers(p.getKey())) tokens += p.getValue();
                }
                if (tokens <= 0) continue;
                e.getValue().signals++;
                wake(e.getValue().parked, tokens);
            }
        } finally {
            lock.unlock();
        }
    }

    /** Opens a watch for one poll that can take {@code capacity} tokens {@code filter} admits. Open it
     *  before the poll's first claim, so no signal from then on is missed. */
    Watch watch(ClaimFilter filter, int capacity) {
        Interest interest = Interest.of(filter);
        lock.lock();
        try {
            Line line = lines.computeIfAbsent(interest, k -> new Line());
            line.watches++;
            return new Watch(interest, line, Math.max(1, capacity));
        } finally {
            lock.unlock();
        }
    }

    /** Wakes polls from the front of {@code parked} until their capacity covers {@code tokens}, then
     *  nudges the new front so it starts its fallback clock. Caller holds the lock. */
    private static void wake(ArrayDeque<Waiter> parked, int tokens) {
        int left = tokens;
        while (left > 0 && !parked.isEmpty()) {
            Waiter w = parked.pollFirst();
            w.woken = true;
            w.wake.signal();
            left -= w.capacity;
        }
        Waiter front = parked.peekFirst();
        if (front != null) front.wake.signal();
    }

    /** The tokens one transaction made ready, tallied until {@link #signal} wakes polls for them. Not
     *  thread-safe: it belongs to the transaction that opened it. */
    final class Batch {

        private final Map<Ready, Integer> produced = new HashMap<>();

        private Batch() {}

        /** One token made ready on {@code queue}, of {@code version} and with {@code facts} (each null
         *  when not known). A null queue is a no-op. */
        void ready(String queue, WorkflowVersion version, TaskFacts facts) {
            if (queue != null) produced.merge(new Ready(queue, version, facts), 1, Integer::sum);
        }

        /** Forgets every token collected, for a transaction attempt that rolled back and is replayed. */
        void clear() {
            produced.clear();
        }

        /** Wakes enough parked polls to claim the tokens collected. */
        void signal() {
            DispatchNotifier.this.signal(produced);
        }
    }

    /** One poll's watch of its line. Not thread-safe: it belongs to the poll that opened it. */
    final class Watch implements AutoCloseable {

        private final Interest interest;
        private final Line line;
        private final int capacity;
        private long seen;
        private boolean closed;

        private Watch(Interest interest, Line line, int capacity) {
            this.interest = interest;
            this.line = line;
            this.capacity = capacity;
            this.seen = line.signals;
        }

        /**
         * Parks the poll until it should claim. Returns true when a signal woke it, or at once when one
         * landed since the watch opened or the last await returned; false when it should claim anyway
         * -- it has been at the front of its line for {@code fallbackMillis} -- or {@code deadline}
         * has passed.
         */
        boolean await(long fallbackMillis, long deadline) {
            lock.lock();
            try {
                if (line.signals > seen) return true;
                Waiter me = new Waiter(lock.newCondition(), capacity);
                line.parked.addLast(me);
                long frontSince = -1;
                try {
                    while (!me.woken) {
                        long now = System.currentTimeMillis();
                        long waitMillis = deadline - now;
                        if (line.parked.peekFirst() == me) {
                            if (frontSince < 0) frontSince = now;
                            waitMillis = Math.min(waitMillis, frontSince + fallbackMillis - now);
                        }
                        if (waitMillis <= 0) return false;
                        me.wake.await(waitMillis, TimeUnit.MILLISECONDS);
                    }
                    return true;
                } finally {
                    if (!me.woken) {
                        boolean wasFront = line.parked.peekFirst() == me;
                        line.parked.remove(me);
                        if (wasFront && !line.parked.isEmpty()) line.parked.peekFirst().wake.signal();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            } finally {
                seen = line.signals;
                lock.unlock();
            }
        }

        /** Wakes one more parked poll of this line: a claim came back full, or a wake went unused. */
        void passOn() {
            lock.lock();
            try {
                wake(line.parked, 1);
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void close() {
            lock.lock();
            try {
                if (closed) return;
                closed = true;
                if (--line.watches == 0) lines.remove(interest, line);
            } finally {
                lock.unlock();
            }
        }
    }
}
