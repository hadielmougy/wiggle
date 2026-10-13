package com.wiggle.server.engine;

import com.wiggle.server.store.ClaimFilter;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** How long a long poll waits between fallback claims, and what a narrowed poll's worker carries
 *  from one poll to the next. */
final class FallbackPollPolicy {

    /** How long the poll at the front of a line of parked polls waits between fallback DB claims
     *  when no local wake-on-produce arrives; the polls behind it wait for a wake (see
     *  {@link DispatchNotifier}). Same-node production wakes a poller immediately; this bounds the
     *  latency for cross-node production (and any missed signal). Overridable via
     *  {@code WIGGLE_FALLBACK_POLL_MILLIS} or {@code wiggle.fallback.pollMillis}. */
    private final long fallbackPollMillis;

    /** The longest a narrowed poll -- one scoped to versions, or with criteria that test its tasks --
     *  waits between fallback claims. Each fallback claim that finds nothing doubles its wait, up to
     *  this, and a claim that finds work resets it; the wait carries over between a worker's polls.
     *  A same-node wake for work it would take still claims at once, so only work produced on
     *  another node can wait this long. Such a claim filters a queue's ready rows, and when few or
     *  none match it can read them all. Overridable via {@code WIGGLE_SCOPED_FALLBACK_MAX_MILLIS}
     *  or {@code wiggle.fallback.scopedMaxMillis}. */
    private final long scopedFallbackMaxMillis;

    /** Adaptive fallback ramp (opt-in): a freshly-parked unnarrowed poll re-claims quickly
     *  (fallback÷4, floor 10ms) and doubles its wait on every empty unsignaled round up to the
     *  configured interval. A notifier signal jumps the wait straight to the configured interval.
     *  At most two extra claims per park, independent of load. Enabled via
     *  {@code WIGGLE_ADAPTIVE_FALLBACK_POLL} or {@code wiggle.adaptive.fallback}. */
    private final boolean adaptiveFallbackPoll;

    /** The current fallback wait of each narrowed poll's worker whose fallback claims found nothing. */
    private final Map<String, Long> scopedFallback = new ConcurrentHashMap<>();

    private final PollerRegistry pollers;

    FallbackPollPolicy(PollerRegistry pollers) {
        this(pollers,
                ServerEnv.envLong("wiggle.fallback.pollMillis", "WIGGLE_FALLBACK_POLL_MILLIS", 100),
                ServerEnv.envLong("wiggle.fallback.scopedMaxMillis", "WIGGLE_SCOPED_FALLBACK_MAX_MILLIS", 2_000),
                Boolean.parseBoolean(System.getProperty("wiggle.adaptive.fallback",
                        System.getenv().getOrDefault("WIGGLE_ADAPTIVE_FALLBACK_POLL", "false"))));
    }

    FallbackPollPolicy(PollerRegistry pollers, long fallbackPollMillis, long scopedFallbackMaxMillis,
                       boolean adaptiveFallbackPoll) {
        this.pollers = pollers;
        this.fallbackPollMillis = fallbackPollMillis;
        this.scopedFallbackMaxMillis = scopedFallbackMaxMillis;
        this.adaptiveFallbackPoll = adaptiveFallbackPoll;
    }

    /** Opens the schedule of one poll by {@code workerId} for what {@code filter} admits. */
    Schedule schedule(String workerId, ClaimFilter filter) {
        boolean scoped = !filter.versions().isEmpty()
                || filter.queues().stream().anyMatch(criteria -> criteria.where() != null);
        boolean adaptive = !scoped && adaptiveFallbackPoll;
        long initialWait = scoped ? scopedFallback.getOrDefault(workerId, fallbackPollMillis)
                : adaptive ? Math.max(10, fallbackPollMillis / 4) : fallbackPollMillis;
        long waitCap = scoped ? scopedFallbackMaxMillis : fallbackPollMillis;
        return new Schedule(workerId, scoped, adaptive, initialWait, waitCap);
    }

    /** One poll's fallback wait. Not thread-safe: it belongs to the poll that opened it. */
    final class Schedule {
        private final String workerId;
        private final boolean scoped;
        private final boolean adaptive;
        private final long waitCap;
        private long waitMillis;

        private Schedule(String workerId, boolean scoped, boolean adaptive, long waitMillis, long waitCap) {
            this.workerId = workerId;
            this.scoped = scoped;
            this.adaptive = adaptive;
            this.waitMillis = waitMillis;
            this.waitCap = waitCap;
        }

        /** How long the poll waits, at the front of its line, before its next fallback claim. */
        long waitMillis() {
            return waitMillis;
        }

        /** Adjusts the wait after one of the poll's claims that followed an await, {@code signaled}
         *  when a signal ended that await. */
        void afterClaim(boolean signaled) {
            if (!signaled && (scoped || adaptive)) {
                waitMillis = Math.min(waitMillis * 2, waitCap);
            } else if (signaled && adaptive) {
                waitMillis = fallbackPollMillis;
            }
        }

        /** Ends the poll: a narrowed poll's worker keeps its wait when the poll found nothing, and
         *  starts over when it found work. */
        void finish(boolean foundWork) {
            if (scoped) rememberFallback(workerId, foundWork ? 0 : waitMillis);
        }
    }

    /** Keeps {@code wait} as the worker's next fallback wait; 0 forgets it. Entries of workers that
     *  stopped polling are dropped once there are many. */
    private void rememberFallback(String workerId, long wait) {
        if (wait <= fallbackPollMillis) {
            scopedFallback.remove(workerId);
            return;
        }
        scopedFallback.put(workerId, wait);
        if (scopedFallback.size() > 1_024) {
            Set<String> live = new HashSet<>();
            for (PollerRegistry.Poller p : pollers.live(System.currentTimeMillis())) live.add(p.workerId());
            scopedFallback.keySet().retainAll(live);
        }
    }
}
