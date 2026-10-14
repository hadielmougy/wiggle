package com.wiggle.server.engine;

import com.wiggle.core.TaskActivation;

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

    /** Runs {@code claim} until it finds work, the request deadline passes, or the worker is gone. */
    Result poll(PollRequest request, Supplier<List<TaskActivation>> claim) {
        if (request.cancelled().cancelled()) {
            return new Result(List.of(), true);
        }

        FallbackPollPolicy.Schedule fallback = fallbackPolicy.schedule(request.workerId(), request.filter());
        try (DispatchNotifier.Watch watch = notifier.watch(request.filter(), request.max(), fallback)) {
            Result result = claimUntilWork(request, watch, claim);
            if (!result.cancelled()) {
                fallback.finish(!result.tasks().isEmpty());
            }
            return result;
        }
    }

    private Result claimUntilWork(
            PollRequest request,
            DispatchNotifier.Watch watch,
            Supplier<List<TaskActivation>> claim
    ) {
        Predicate<List<TaskActivation>> isFull =
                tasks -> !tasks.isEmpty() && tasks.size() >= request.max();
        List<TaskActivation> tasks = watch.claim(claim, isFull);

        while (tasks.isEmpty() && System.currentTimeMillis() < request.deadline()) {
            boolean signaled = watch.await(request.deadline());

            if (signaled && request.max() > 1) {
                lingerForBatch(request.deadline());
            }

            if (request.cancelled().cancelled()) {
                watch.forgo();
                return new Result(List.of(), true);
            }

            tasks = watch.claim(claim, isFull);
        }

        return new Result(tasks, false);
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
