package com.wiggle.observe;

import com.wiggle.core.RecordMapper;
import com.wiggle.observe.Run.Batch;
import com.wiggle.proto.ObserveManyRequest;
import com.wiggle.proto.ObserveManyResult;
import com.wiggle.proto.ObserveOutcome;
import com.wiggle.proto.ObserveRunRequest;
import com.wiggle.proto.ProtoJson;
import com.wiggle.proto.StepResult;
import com.wiggle.proto.WiggleControlPlaneGrpc;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The one thread that talks to the server. Reports queue here and travel in {@code ObserveMany}
 * calls, at most one batch per run per call so a run's second report can name the instance its
 * first one minted. Nothing the application does waits on the network: a full queue drops the
 * report and counts it, and a call that fails drops its batches and marks their runs lost.
 */
final class Reporter implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(Reporter.class.getName());
    private static final int MAX_PER_CALL = 256;

    /** Where a run's reports go: one address in direct mode, the owner cell of the run's key under a coordinator. */
    private final Function<Run, String> targetOf;
    private final Function<String, WiggleControlPlaneGrpc.WiggleControlPlaneBlockingStub> stubFor;
    private final ObserverOptions options;
    private final LinkedBlockingQueue<Batch> queue;
    private final Set<Run> pending = new HashSet<>();
    private final AtomicLong dropped = new AtomicLong();
    private final Thread flusher;
    private volatile boolean closing;
    private boolean warned;

    Reporter(Function<Run, String> targetOf,
             Function<String, WiggleControlPlaneGrpc.WiggleControlPlaneBlockingStub> stubFor, ObserverOptions options) {
        this.targetOf = targetOf;
        this.stubFor = stubFor;
        this.options = options;
        this.queue = new LinkedBlockingQueue<>(options.queueCapacity());
        this.flusher = Thread.ofPlatform().name("wiggle-observe-flusher").daemon(true).start(this::loop);
    }

    void submit(Batch batch) {
        if (batch.run().dead()) return;
        if (!queue.offer(batch)) {
            dropped.addAndGet(batch.steps().size());
            batch.run().lost();
            LOG.log(System.Logger.Level.WARNING, () -> "observe queue full (" + options.queueCapacity()
                    + "); dropped " + batch.steps().size() + " step(s) of a run of " + batch.run().owner().name());
        }
    }

    void pending(Run run) {
        synchronized (pending) {
            pending.add(run);
        }
    }

    long dropped() {
        return dropped.get();
    }

    private void loop() {
        long tick = Math.max(1, options.linger().toMillis() / 2);
        while (!closing || !queue.isEmpty()) {
            try {
                Batch first = queue.poll(tick, TimeUnit.MILLISECONDS);
                lingered();
                if (first == null) continue;
                send(drain(first));
            } catch (InterruptedException e) {
                return;
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.WARNING, "observe flusher: " + e);
            }
        }
    }

    /** Moves runs whose buffered steps have waited past the linger onto the queue. */
    private void lingered() {
        List<Run> stale;
        synchronized (pending) {
            if (pending.isEmpty()) return;
            stale = new ArrayList<>(pending);
        }
        long now = System.currentTimeMillis();
        for (Run run : stale) {
            Batch b = run.takeIfStale(options.linger().toMillis(), now, pending);
            if (b != null) submit(b);
        }
    }

    /** One call's worth: distinct runs only; a run's second batch waits for the next call. */
    private List<Batch> drain(Batch first) {
        List<Batch> call = new ArrayList<>();
        List<Batch> deferred = new ArrayList<>();
        Set<Run> runs = new HashSet<>();
        Batch b = first;
        while (b != null && call.size() < MAX_PER_CALL) {
            if (runs.add(b.run())) call.add(b);
            else deferred.add(b);
            b = queue.poll();
        }
        if (b != null) deferred.add(b);
        for (Batch d : deferred) {
            if (!queue.offer(d)) { dropped.addAndGet(d.steps().size()); d.run().lost(); }
        }
        return call;
    }

    /** One call per target: under a coordinator the runs in a drain may live on different cells. */
    private void send(List<Batch> drained) {
        Map<String, List<Batch>> byTarget = new LinkedHashMap<>();
        for (Batch b : drained) {
            String target;
            try {
                target = targetOf.apply(b.run());
            } catch (RuntimeException e) {
                dropped.addAndGet(b.steps().size());
                b.run().lost();
                LOG.log(System.Logger.Level.WARNING, () -> "observe: cannot resolve where run '"
                        + b.run().correlationId() + "' of " + b.run().owner().name() + " lives: " + e);
                continue;
            }
            byTarget.computeIfAbsent(target, t -> new ArrayList<>()).add(b);
        }
        byTarget.forEach(this::sendTo);
    }

    private void sendTo(String target, List<Batch> call) {
        ObserveManyRequest.Builder req = ObserveManyRequest.newBuilder();
        for (Batch b : call) req.addRuns(request(b));
        ObserveManyResult res;
        try {
            res = stubFor.apply(target).observeMany(req.build());
            warned = false;
        } catch (RuntimeException e) {
            long lost = 0;
            for (Batch b : call) { lost += b.steps().size(); b.run().lost(); }
            dropped.addAndGet(lost);
            long n = lost;
            LOG.log(warned ? System.Logger.Level.DEBUG : System.Logger.Level.WARNING,
                    () -> "observe report failed, dropped " + n + " step(s) of " + call.size() + " run(s): " + e);
            warned = true;
            return;
        }
        for (int i = 0; i < call.size() && i < res.getResultsCount(); i++) {
            ObserveOutcome o = res.getResults(i);
            Run run = call.get(i).run();
            if (o.getErrorStatus() == 0) {
                run.landed(o.getOutcome().getInstanceId(), o.getOutcome().getInstanceStatus());
            } else {
                dropped.addAndGet(call.get(i).steps().size());
                run.lost();
                LOG.log(System.Logger.Level.WARNING, () -> "observe report refused (" + o.getErrorStatus()
                        + "): " + o.getError());
            }
        }
    }

    private ObserveRunRequest request(Batch b) {
        Run run = b.run();
        // Every report names the key: that is what lets a service that never saw the first report
        // land on the same instance. The instance id, once known, only spares the server a lookup.
        ObserveRunRequest.Builder req = ObserveRunRequest.newBuilder()
                .setWorkflow(run.owner().name())
                .setVersion(run.owner().version())
                .setReporter(options.reporter())
                .setCorrelationId(run.correlationId())
                .setFinal(b.fin());
        if (run.instanceId() != null) req.setInstanceId(run.instanceId());
        for (StepRecord s : b.steps()) {
            StepResult.Builder sr = StepResult.newBuilder().setNodeId(s.nodeId())
                    .setStartedAt(s.startedAt()).setFinishedAt(s.finishedAt());
            if (s.afterNode() != null) sr.setAfterNode(s.afterNode());
            if (s.error() != null) sr.setError(s.error());
            else if (s.predicateValue() != null) sr.setPredicateValue(s.predicateValue());
            else if (s.merge() != null) sr.setMerge(ProtoJson.toValue(RecordMapper.toJson(s.merge())));
            req.addSteps(sr);
        }
        return req.build();
    }

    /** Sends what is queued, then stops; waits at most {@code grace} for the flusher to finish. */
    void close(long graceMillis) {
        closing = true;
        try {
            flusher.join(graceMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        flusher.interrupt();
    }

    @Override public void close() {
        close(5_000);
    }
}
