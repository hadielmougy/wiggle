package com.wiggle.client.worker;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.WiggleClient.RunOutcome;
import com.wiggle.client.WiggleClient.RunSubmission;
import com.wiggle.core.AdvanceResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Collapses concurrent final handbacks into one {@code AdvanceMany} call. A LOCAL_ASYNC run that
 * ends at a boundary needs nothing back but durability, so those flushes -- unlike mid-chain
 * ones, which need the leased continuation id synchronously -- can travel together: the
 * submitting thread parks on its future, a short linger lets the worker's other in-flight runs
 * land in the same window, and one RPC carries them all. The ack still happens before the
 * worker's slot frees, so a crash means exactly what it meant before: unacked work re-runs from
 * the last flush when the lease expires.
 *
 * <p>A run the batch refuses (another arm of the same instance in the same window, say) or a
 * batch that fails wholesale falls back to the single-run path in the submitting thread, so the
 * worker's behaviour is byte-for-byte what it was -- batching changes what the same work costs,
 * never what it does.
 */
final class HandbackBatcher implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(HandbackBatcher.class.getName());
    private static final long LINGER_MILLIS = 2;

    private record Pending(RunSubmission run, CompletableFuture<AdvanceResult> done) {}

    private final WiggleClient client;
    private final int maxBatch;
    private final BlockingQueue<Pending> queue;
    private final Thread flusher;
    private volatile boolean running = true;

    HandbackBatcher(WiggleClient client, int maxBatch) {
        this.client = client;
        this.maxBatch = Math.max(1, maxBatch);
        this.queue = new ArrayBlockingQueue<>(this.maxBatch * 8);
        this.flusher = new Thread(this::flushLoop, "wiggle-handback-batcher");
        this.flusher.setDaemon(true);
        this.flusher.start();
    }

    /** Reports one final handback, batched; blocks until it is durable on the server. */
    AdvanceResult handback(String taskId, String leaseOwner, List<WiggleClient.StepReport> steps) {
        RunSubmission run = new RunSubmission(taskId, leaseOwner, steps, true);
        CompletableFuture<AdvanceResult> done = new CompletableFuture<>();
        if (!running || !queue.offer(new Pending(run, done))) {
            return single(run);   // closing, or the queue is saturated: the old path is always there
        }
        try {
            return done.join();
        } catch (RuntimeException e) {
            return single(run);   // the batch failed wholesale or refused this run; report it alone
        }
    }

    private AdvanceResult single(RunSubmission run) {
        return client.advanceRun(run.taskId(), run.leaseOwner(), run.steps(), true);
    }

    private void flushLoop() {
        List<Pending> batch = new ArrayList<>(maxBatch);
        while (running || !queue.isEmpty()) {
            batch.clear();
            try {
                Pending first = queue.poll(50, TimeUnit.MILLISECONDS);
                if (first == null) continue;
                batch.add(first);
                long deadline = System.nanoTime() + LINGER_MILLIS * 1_000_000;
                while (batch.size() < maxBatch) {
                    long left = deadline - System.nanoTime();
                    Pending next = left > 0 ? queue.poll(left, TimeUnit.NANOSECONDS) : queue.poll();
                    if (next == null) break;
                    batch.add(next);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            flush(batch);
        }
        for (Pending p : queue) p.done().completeExceptionally(new IllegalStateException("batcher closed"));
    }

    private void flush(List<Pending> batch) {
        try {
            Map<String, RunOutcome> results = client.advanceMany(
                    batch.stream().map(Pending::run).toList());
            for (Pending p : batch) {
                RunOutcome r = results.get(p.run().taskId());
                if (r != null && r.ok()) {
                    p.done().complete(r.outcome());
                } else {
                    // Refused without writing (or unanswered): the submitting thread retries singly.
                    p.done().completeExceptionally(new IllegalStateException(
                            r == null ? "no result for run" : r.errorStatus() + ": " + r.error()));
                }
            }
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.DEBUG, () -> "handback batch of " + batch.size()
                    + " fell back to single reports: " + e);
            for (Pending p : batch) p.done().completeExceptionally(e);
        }
    }

    @Override
    public void close() {
        running = false;
        flusher.interrupt();
        try {
            flusher.join(5_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
