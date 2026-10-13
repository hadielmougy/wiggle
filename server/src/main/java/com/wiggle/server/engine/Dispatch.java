package com.wiggle.server.engine;

import com.wiggle.core.TaskActivation;
import com.wiggle.server.store.ClaimFilter;
import com.wiggle.server.store.StorageUnreachableException;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Handing work to workers: one claim across the instance shards via {@link Tokens#claim}, run by a
 * {@link PollSession} that decides WHEN to claim. Nothing here decides a token's fate.
 */
final class Dispatch {

    private static final System.Logger LOG = System.getLogger(Dispatch.class.getName());

    private final Transactions transactions;
    private final Tokens tokens;
    private final PollerRegistry pollers;
    private final PollSession pollSession;
    private final long defaultLeaseMillis;
    /** Where the next claim starts, so polls spread over the instance shards. */
    private final AtomicInteger nextShard = new AtomicInteger();

    Dispatch(Transactions transactions, Tokens tokens, DispatchNotifier notifier,
             PollerRegistry pollers, long defaultLeaseMillis) {
        this.transactions = transactions;
        this.tokens = tokens;
        this.pollers = pollers;
        this.pollSession = new PollSession(notifier, new FallbackPollPolicy(pollers));
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
        long lease = leaseMillis == null || leaseMillis <= 0 ? defaultLeaseMillis : leaseMillis;
        PollSession.Result result = pollSession.poll(workerId, filter, max, deadline, cancelled,
                () -> claimNow(workerId, filter, max, lease));
        if (result.cancelled()) return List.of();
        List<TaskActivation> tasks = result.tasks();
        logClaim(workerId, filter.queueNames(), tasks);
        return tasks;
    }

    private void logClaim(String workerId, Set<String> queues, List<TaskActivation> tasks) {
        if (!tasks.isEmpty()) {
            LOG.log(System.Logger.Level.DEBUG, () -> "poll: worker " + workerId + " queues=" + queues
                    + " claimed " + tasks.size() + " task(s): "
                    + tasks.stream().map(a -> a.taskId() + "@" + a.stepName()).toList());
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
