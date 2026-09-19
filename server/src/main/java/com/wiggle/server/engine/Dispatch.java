package com.wiggle.server.engine;

import com.wiggle.core.TaskActivation;
import com.wiggle.core.WorkflowVersion;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;

/**
 * Handing work to workers: the long poll's pacing around {@link TokenLifecycle#claim}. Nothing
 * here decides a token's fate -- it decides only WHEN to go look for one, trading claims against
 * discovery latency (see docs/in-memory-dispatch.md).
 */
final class Dispatch {

    private static final System.Logger LOG = System.getLogger(Dispatch.class.getName());

    /** How long a long-poll waits between fallback DB claims when no local wake-on-produce arrives.
     *  Same-node production wakes a poller immediately; this bounds the latency for cross-node
     *  production (and any missed signal). Overridable via {@code WIGGLE_FALLBACK_POLL_MILLIS}. */
    private final long fallbackPollMillis = ServerEnv.envLong("WIGGLE_FALLBACK_POLL_MILLIS", 100);

    /** Adaptive fallback ramp (opt-in): a freshly-parked poll re-claims quickly (fallback÷4, floor
     *  10ms) and doubles its wait on every empty unsignaled round up to the configured interval.
     *  Work produced on ANOTHER node shortly after this one parks — the common case under steady
     *  load, where a poller re-parks right before the next task lands — is discovered in the fast
     *  window instead of a uniform [0, fallback) delay. A notifier signal jumps the wait straight to
     *  the configured interval: a signal proves the LOCAL wake path is covering this node, so fast
     *  re-claims add DB load without adding discovery (an earlier reset-to-fast-on-signal kept polls
     *  in fast mode on busy nodes and measurably cost ceiling throughput). Extra cost is therefore
     *  ≤2 claims per park, independent of load. */
    private final boolean adaptiveFallbackPoll = Boolean.parseBoolean(
            System.getProperty("wiggle.adaptive.fallback",
                    System.getenv().getOrDefault("WIGGLE_ADAPTIVE_FALLBACK_POLL", "false")));

    /** After a wake-on-produce signal, briefly let more tokens accumulate before claiming, so a burst
     *  is drained in one batched claim instead of a round trip per token. Trades up to this much
     *  first-token latency for fewer, larger claims under load; 0 disables (claim immediately). Only
     *  applies when the worker asked for more than one task (it has spare capacity to batch).
     *  Overridable via {@code WIGGLE_DISPATCH_LINGER_MILLIS}. */
    private final long dispatchLingerMillis = ServerEnv.envLong("WIGGLE_DISPATCH_LINGER_MILLIS", 5);

    private final Transactions transactions;
    private final TokenLifecycle tokens;
    private final DispatchNotifier notifier;
    private final PollerRegistry pollers;
    private final long defaultLeaseMillis;

    Dispatch(Transactions transactions, TokenLifecycle tokens, DispatchNotifier notifier,
             PollerRegistry pollers, long defaultLeaseMillis) {
        this.transactions = transactions;
        this.tokens = tokens;
        this.notifier = notifier;
        this.pollers = pollers;
        this.defaultLeaseMillis = defaultLeaseMillis;
    }

    /**
     * Long-polls for work. {@code cancelled} lets the caller (the gRPC layer) signal that the worker's
     * request is gone -- a closing or dead worker whose call was cancelled -- so we do not claim a
     * token for a worker that will never run it (which would only strand it until lease expiry). This
     * matters with wake-on-produce: a signal can wake a parked poll the instant its worker is shutting
     * down, and the freshly-produced token should go to a live worker instead.
     */
    List<TaskActivation> poll(String workerId, Set<String> queues, Set<WorkflowVersion> versions,
                              int max, Long leaseMillis, long deadline, BooleanSupplier cancelled) {
        pollers.seen(workerId, queues, versions, System.currentTimeMillis());
        long lease = leaseMillis == null || leaseMillis <= 0 ? defaultLeaseMillis : leaseMillis;
        if (cancelled.getAsBoolean()) return List.of();
        Map<String, Long> since = notifier.snapshot(queues);
        List<TaskActivation> tasks = claimNow(workerId, queues, versions, max, lease);
        long rampStart = Math.max(10, fallbackPollMillis / 4);
        long fallbackWait = adaptiveFallbackPoll ? rampStart : fallbackPollMillis;
        while (tasks.isEmpty() && System.currentTimeMillis() < deadline) {
            long remaining = deadline - System.currentTimeMillis();
            boolean signaled = notifier.awaitChange(queues, since, Math.min(fallbackWait, remaining));
            if (signaled && max > 1) lingerForBatch(deadline);
            if (cancelled.getAsBoolean()) return List.of();   // worker gone -- leave the work for a live one
            since = notifier.snapshot(queues);
            tasks = claimNow(workerId, queues, versions, max, lease);
            if (adaptiveFallbackPoll) {
                fallbackWait = signaled ? fallbackPollMillis : Math.min(fallbackWait * 2, fallbackPollMillis);
            }
        }
        if (!tasks.isEmpty()) {
            List<TaskActivation> claimed = tasks;
            LOG.log(System.Logger.Level.DEBUG, () -> "poll: worker " + workerId + " queues=" + queues
                    + " claimed " + claimed.size() + " task(s): "
                    + claimed.stream().map(a -> a.taskId() + "@" + a.stepName()).toList());
        }
        return tasks;
    }

    /** One atomic DB claim attempt, with a lease that starts now (not at the poll's arrival). */
    private List<TaskActivation> claimNow(String workerId, Set<String> queues,
                                          Set<WorkflowVersion> versions, int max, long lease) {
        long now = System.currentTimeMillis();
        return transactions.read(tx -> tokens.claim(tx, workerId, queues, versions, max, now, now + lease));
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
}
