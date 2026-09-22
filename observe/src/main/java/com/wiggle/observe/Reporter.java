package com.wiggle.observe;

import com.wiggle.proto.ObserveManyRequest;
import com.wiggle.proto.ObserveManyResult;
import com.wiggle.proto.ObserveOutcome;
import com.wiggle.proto.ObserveRunRequest;
import com.wiggle.proto.StepResult;
import com.wiggle.proto.WiggleControlPlaneGrpc;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The one thread that talks to the server. Reports queue here and travel together in
 * {@code ObserveMany} calls, one run per report, in the order they were made. Nothing the
 * application does waits on the network: a full queue drops the report and counts it, and a call
 * that fails drops what it carried.
 */
final class Reporter implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(Reporter.class.getName());
    private static final int MAX_PER_CALL = 256;
    /** Wakes the flusher on close so what is queued goes before the connection does. */
    private static final Report STOP = new Report(null, 0, null, null, null, null, 0, 0, false);

    private final WiggleControlPlaneGrpc.WiggleControlPlaneBlockingStub stub;
    private final ObserverOptions options;
    private final LinkedBlockingQueue<Report> queue;
    private final AtomicLong dropped = new AtomicLong();
    private final Thread flusher;
    private volatile boolean closing;
    private boolean warned;

    Reporter(WiggleControlPlaneGrpc.WiggleControlPlaneBlockingStub stub, ObserverOptions options) {
        this.stub = stub;
        this.options = options;
        this.queue = new LinkedBlockingQueue<>(options.queueCapacity());
        this.flusher = Thread.ofPlatform().name("wiggle-observe-flusher").daemon(true).start(this::loop);
    }

    void submit(Report r) {
        if (!queue.offer(r)) {
            dropped.incrementAndGet();
            LOG.log(System.Logger.Level.WARNING, () -> "observe queue full (" + options.queueCapacity()
                    + "); dropped a report of run '" + r.key() + "' of " + r.workflow());
        }
    }

    long dropped() {
        return dropped.get();
    }

    private void loop() {
        long linger = Math.max(1, options.linger().toMillis());
        while (true) {
            Report first;
            try {
                first = queue.poll(linger, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                flushRemaining();
                return;
            }
            if (first == null || first == STOP) {
                if (closing) { flushRemaining(); return; }
                continue;
            }
            List<Report> call = new ArrayList<>();
            call.add(first);
            // Let the reports made right behind this one travel with it -- unless we are closing,
            // in which case what is here goes now.
            long deadline = System.nanoTime() + linger * 1_000_000;
            while (call.size() < MAX_PER_CALL && !closing) {
                long left = deadline - System.nanoTime();
                if (left <= 0) break;
                Report next;
                try {
                    next = queue.poll(left, TimeUnit.NANOSECONDS);
                } catch (InterruptedException e) {
                    break;
                }
                if (next == null || next == STOP) break;
                call.add(next);
            }
            queue.drainTo(call, MAX_PER_CALL - call.size());
            call.removeIf(r -> r == STOP);
            send(call);
            if (closing) { flushRemaining(); return; }
        }
    }

    /** Best effort on the way out: whatever is still queued goes in as many calls as it takes. */
    private void flushRemaining() {
        while (!queue.isEmpty()) {
            List<Report> call = new ArrayList<>();
            queue.drainTo(call, MAX_PER_CALL);
            call.removeIf(r -> r == STOP);
            if (!call.isEmpty()) send(call);
        }
    }

    private void send(List<Report> call) {
        ObserveManyRequest.Builder req = ObserveManyRequest.newBuilder();
        for (Report r : call) req.addRuns(request(r));
        ObserveManyResult res;
        try {
            res = stub.observeMany(req.build());
            warned = false;
        } catch (RuntimeException e) {
            dropped.addAndGet(call.size());
            LOG.log(warned ? System.Logger.Level.DEBUG : System.Logger.Level.WARNING,
                    () -> "observe report failed, dropped " + call.size() + " report(s): " + e);
            warned = true;
            return;
        }
        for (int i = 0; i < call.size() && i < res.getResultsCount(); i++) {
            ObserveOutcome o = res.getResults(i);
            if (o.getErrorStatus() != 0) {
                dropped.incrementAndGet();
                LOG.log(System.Logger.Level.WARNING, () -> "observe report refused (" + o.getErrorStatus() + "): " + o.getError());
            }
        }
    }

    private ObserveRunRequest request(Report r) {
        ObserveRunRequest.Builder req = ObserveRunRequest.newBuilder()
                .setWorkflow(r.workflow())
                .setVersion(r.version())
                .setReporter(options.reporter())
                .setCorrelationId(r.key())
                .setFinal(r.fin());
        if (r.nodeId() != null) {
            StepResult.Builder sr = StepResult.newBuilder().setNodeId(r.nodeId())
                    .setStartedAt(r.startedAt()).setFinishedAt(r.finishedAt());
            if (r.error() != null) sr.setError(r.error());
            else if (r.predicateValue() != null) sr.setPredicateValue(r.predicateValue());
            req.addSteps(sr);
        }
        return req.build();
    }

    /** Sends what is queued, then stops; waits at most {@code graceMillis} for the flusher to finish. */
    void close(long graceMillis) {
        closing = true;
        queue.offer(STOP);   // a full queue needs no wake-up: its poll returns at once
        try {
            flusher.join(graceMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (flusher.isAlive()) flusher.interrupt();
    }

    @Override public void close() {
        close(5_000);
    }
}
