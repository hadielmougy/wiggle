package com.wiggle.client.worker;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.WiggleClient.RunOutcome;
import com.wiggle.client.WiggleClient.RunSubmission;
import com.wiggle.core.ReportResult;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Collapses concurrent final handbacks into one {@code ReportSteps} call. A LOCAL_ASYNC run that
 * ends at a boundary needs nothing back but durability, so those flushes -- unlike mid-chain
 * ones, which need the leased continuation id synchronously -- can travel together.
 *
 * <p>No flusher thread and no linger: the first submitting thread to arrive becomes the leader,
 * drains whatever its peers have queued <em>right now</em>, and sends one call; whoever queues
 * while that call is in flight is picked up by the next leader. Batch size scales with load by
 * itself, and an idle worker pays a single-run call's latency, nothing more. (A dedicated
 * flusher was measured and rejected: it re-created the single-flusher serialisation that sank
 * server-side group commit, one connection's latency gating every handback.)
 *
 * <p>Two runs of one INSTANCE never share a batch -- the second stays queued for the next
 * leader, because the server refuses same-instance batch-mates (their writes would interleave)
 * and a refusal here would sit on the join's critical path. The ack still happens before the
 * worker's slot frees, so a crash means exactly what it meant before: unacked work re-runs from
 * the last flush when the lease expires. A run the batch still refuses, or a batch that dies
 * wholesale, falls back to the single-run path in the submitting thread.
 */
final class HandbackBatcher {

    private static final System.Logger LOG = System.getLogger(HandbackBatcher.class.getName());

    private record Pending(String instanceId, RunSubmission run, CompletableFuture<ReportResult> done) {}

    private final WiggleClient client;
    private final int maxBatch;
    private final ConcurrentLinkedQueue<Pending> queue = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean leading = new AtomicBoolean();

    HandbackBatcher(WiggleClient client, int maxBatch) {
        this.client = client;
        this.maxBatch = Math.max(1, maxBatch);
    }

    /** Reports one final handback, batched; blocks until it is durable on the server. */
    ReportResult handback(String instanceId, String taskId, String leaseOwner,
                           List<WiggleClient.StepReport> steps) {
        RunSubmission run = new RunSubmission(taskId, leaseOwner, steps, true);
        CompletableFuture<ReportResult> done = new CompletableFuture<>();
        queue.add(new Pending(instanceId, run, done));
        // A follower never parks unbounded: the leader that is flushing right now may have
        // deferred this run (its sibling was in that batch), and with no later traffic no one
        // else would lead. Wait briefly, then come back and lead it ourselves.
        while (!done.isDone()) {
            if (leading.compareAndSet(false, true)) {
                try {
                    flushOnce();
                } finally {
                    leading.set(false);
                }
            } else {
                try {
                    done.get(1, java.util.concurrent.TimeUnit.MILLISECONDS);
                } catch (java.util.concurrent.TimeoutException retry) {
                    // loop: try to lead
                } catch (Exception settled) {
                    break;
                }
            }
        }
        try {
            return done.join();
        } catch (RuntimeException e) {
            return single(run);
        }
    }

    /** Drains one instance-disjoint batch and sends it; runs it could not take stay queued. */
    private void flushOnce() {
        List<Pending> batch = new ArrayList<>();
        List<Pending> deferred = new ArrayList<>();
        Set<String> instances = new HashSet<>();
        Pending p;
        while (batch.size() < maxBatch && (p = queue.poll()) != null) {
            if (instances.add(p.instanceId())) batch.add(p);
            else deferred.add(p);          // its sibling is in this batch; the next leader takes it
        }
        queue.addAll(deferred);
        if (batch.isEmpty()) return;
        if (batch.size() == 1) {
            Pending only = batch.getFirst();
            try {
                only.done().complete(single(only.run()));
            } catch (RuntimeException e) {
                only.done().completeExceptionally(e);
            }
            return;
        }
        try {
            Map<String, RunOutcome> results = client.reportSteps(
                    batch.stream().map(Pending::run).toList());
            for (Pending b : batch) {
                RunOutcome r = results.get(b.run().taskId());
                if (r != null && r.ok()) {
                    b.done().complete(r.outcome());
                } else {
                    b.done().completeExceptionally(new IllegalStateException(
                            r == null ? "no result for run" : r.errorStatus() + ": " + r.error()));
                }
            }
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.DEBUG, () -> "handback batch of " + batch.size()
                    + " fell back to single reports: " + e);
            for (Pending b : batch) b.done().completeExceptionally(e);
        }
    }

    private ReportResult single(RunSubmission run) {
        return client.reportSteps(run.taskId(), run.leaseOwner(), run.steps(), true);
    }
}
