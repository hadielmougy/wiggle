package com.wiggle.server.engine;

import com.wiggle.core.TaskActivation;
import com.wiggle.server.store.ClaimFilter;

import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * One long poll's pacing around a claim it is handed: when to claim, when to wait for a wake or a
 * fallback, when to give up. It never claims anything itself (see docs/archive/in-memory-dispatch.md).
 */
final class PollSession {

    /** After a wake-on-produce signal, briefly let more tokens accumulate before claiming, so a burst
     *  is drained in one batched claim instead of a round trip per token. Trades up to this much
     *  first-token latency for fewer, larger claims under load; 0 disables (claim immediately). Only
     *  applies when the worker asked for more than one task. Overridable via
     *  {@code WIGGLE_DISPATCH_LINGER_MILLIS}. */
    private final long dispatchLingerMillis = ServerEnv.envLong("WIGGLE_DISPATCH_LINGER_MILLIS", 5);

    private final DispatchNotifier notifier;
    private final FallbackPollPolicy fallbackPolicy;

    PollSession(DispatchNotifier notifier, FallbackPollPolicy fallbackPolicy) {
        this.notifier = notifier;
        this.fallbackPolicy = fallbackPolicy;
    }

    /** Runs {@code claim} until it finds work, {@code deadline} passes, or {@code cancelled} says the
     *  worker is gone. A claim that throws ends the poll with its exception. */
    Result poll(String workerId, ClaimFilter filter, int max, long deadline,
                Cancellation cancelled, Supplier<List<TaskActivation>> claim) {
        if (cancelled.cancelled()) return new Result(List.of(), true);
        try (DispatchNotifier.Watch watch = notifier.watch(filter, max)) {
            Predicate<List<TaskActivation>> full = got -> !got.isEmpty() && got.size() >= max;
            List<TaskActivation> tasks = watch.claim(claim, full);
            FallbackPollPolicy.Schedule fallback = fallbackPolicy.schedule(workerId, filter);
            while (tasks.isEmpty() && System.currentTimeMillis() < deadline) {
                boolean signaled = watch.await(fallback.waitMillis(), deadline);
                if (signaled && max > 1) lingerForBatch(deadline);
                if (cancelled.cancelled()) {
                    watch.forgo();   // a wake meant for a claim goes to another poll
                    return new Result(List.of(), true); // worker gone; leave work for a live one
                }
                tasks = watch.claim(claim, full);
                fallback.afterClaim(signaled);
            }
            fallback.finish(!tasks.isEmpty());
            return new Result(tasks, false);
        }
    }

    /** Coalesce a burst: wait up to {@link #dispatchLingerMillis} (bounded by the poll deadline) so
     *  concurrently-produced tokens are claimed together instead of one per round trip. */
    private void lingerForBatch(long deadline) {
        if (dispatchLingerMillis <= 0) return;
        long budget = Math.min(dispatchLingerMillis, deadline - System.currentTimeMillis());
        if (budget <= 0) return;
        try {
            Thread.sleep(budget);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** What a poll claimed, or that it was cancelled. */
    record Result(List<TaskActivation> tasks, boolean cancelled) {}
}
