package com.wiggle.server.engine;

import com.wiggle.core.TaskActivation;
import com.wiggle.server.store.ClaimFilter;
import com.wiggle.server.store.StorageUnreachableException;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

/**
 * Handing work to workers: one long poll's pacing through the notifier's {@link DispatchNotifier.Watch}
 * -- when to claim, when to wait for a wake or a fallback, when to give up -- around one claim across
 * the instance shards via {@link Tokens#claim}. Nothing here decides a token's fate (see
 * docs/archive/in-memory-dispatch.md).
 */
final class Dispatch {

    private static final System.Logger LOG = System.getLogger(Dispatch.class.getName());

    private final Transactions transactions;
    private final Tokens tokens;
    private final PollerRegistry pollers;
    private final DispatchNotifier notifier;
    private final FallbackPollPolicy fallbackPolicy;
    private final long defaultLeaseMillis;
    /** After a wake-on-produce signal, briefly let more tokens accumulate before claiming, so a burst
     *  is drained in one batched claim instead of a round trip per token. Trades up to this much
     *  first-token latency for fewer, larger claims under load; 0 disables (claim immediately). Only
     *  applies when the worker asked for more than one task. Overridable via
     *  {@code WIGGLE_DISPATCH_LINGER_MILLIS}. */
    private final long dispatchLingerMillis = ServerEnv.envLong("WIGGLE_DISPATCH_LINGER_MILLIS", 5);
    /** Where the next claim starts, so polls spread over the instance shards. */
    private final AtomicInteger nextShard = new AtomicInteger();

    Dispatch(Transactions transactions, Tokens tokens, DispatchNotifier notifier,
             PollerRegistry pollers, long defaultLeaseMillis) {
        this.transactions = transactions;
        this.tokens = tokens;
        this.pollers = pollers;
        this.notifier = notifier;
        this.fallbackPolicy = new FallbackPollPolicy(pollers);
        this.defaultLeaseMillis = defaultLeaseMillis;
    }

    /**
     * Long-polls for work. {@code cancelled} lets the caller (the gRPC layer) signal that the worker's
     * request is gone -- a closing or dead worker whose call was cancelled -- so we do not claim a
     * token for a worker that will never run it (which would only strand it until lease expiry). This
     * matters with wake-on-produce: a signal can wake a parked poll the instant its worker is shutting
     * down, and the freshly-produced token should go to a live worker instead.
     */
    List<TaskActivation> poll(String workerId, ClaimFilter filter,
                              int max, Long leaseMillis, long deadline, Cancellation cancelled) {
        pollers.seen(workerId, filter, System.currentTimeMillis());
        if (cancelled.cancelled()) {
            return List.of();
        }
        long lease = leaseMillis == null || leaseMillis <= 0 ? defaultLeaseMillis : leaseMillis;
        Predicate<List<TaskActivation>> isFull = tasks -> !tasks.isEmpty() && tasks.size() >= max;

        FallbackPollPolicy.Schedule fallback = fallbackPolicy.schedule(workerId, filter);
        try (DispatchNotifier.Watch watch = notifier.watch(filter, max, fallback)) {
            List<TaskActivation> tasks = watch.claim(() -> claimNow(workerId, filter, max, lease), isFull);

            while (tasks.isEmpty() && System.currentTimeMillis() < deadline) {
                boolean signaled = watch.await(deadline);

                if (signaled && max > 1) {
                    lingerForBatch(deadline);
                }

                if (cancelled.cancelled()) {
                    watch.forgo();
                    return List.of();
                }

                tasks = watch.claim(() -> claimNow(workerId, filter, max, lease), isFull);
            }

            fallback.finish(!tasks.isEmpty());
            return tasks;
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

    /**
     * One claim attempt, with a lease that starts now (not at the poll's arrival): one shard at a
     * time, from a rotating start, taking the first shard that has work. Each shard's claim is atomic.
     * An unreachable shard is passed over; only when every shard is unreachable does the claim fail.
     */
    private List<TaskActivation> claimNow(String workerId, ClaimFilter filter, int max, long lease) {
        List<Integer> shards = transactions.instanceShards();
        int start = Math.floorMod(nextShard.getAndIncrement(), shards.size());
        StorageUnreachableException unreachable = null;
        int reached = 0;
        for (int i = 0; i < shards.size(); i++) {
            long now = System.currentTimeMillis();
            List<TaskActivation> got;
            try {
                got = transactions.readShard(shards.get((start + i) % shards.size()),
                        tx -> tokens.claim(tx, workerId, filter, max, now, now + lease));
            } catch (StorageUnreachableException e) {
                unreachable = e;
                continue;
            }
            reached++;
            if (!got.isEmpty()) return got;
        }
        if (reached == 0 && unreachable != null) throw unreachable;
        return List.of();
    }
}
