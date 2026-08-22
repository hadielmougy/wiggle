package dev.wiggle.client.worker;

import dev.wiggle.client.dsl.ActivityHandler;
import dev.wiggle.client.dsl.Blueprint;
import dev.wiggle.core.AdvanceResult;
import dev.wiggle.core.ExecutionMode;
import dev.wiggle.core.GraphTraversal;
import dev.wiggle.core.Node;
import dev.wiggle.core.NodeKind;
import dev.wiggle.core.TaskActivation;
import dev.wiggle.core.WorkflowDefinition;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The data plane. A worker registers its blueprints, then pulls work: it only ever
 * asks for as many tasks as it has free slots, so the server never overwhelms it and
 * backpressure is a property of the protocol rather than a thing to configure.
 *
 * Workers hold no workflow state. Losing one loses only the in-flight leases, which
 * the server's leader reclaims once they expire.
 */
public final class Worker implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(Worker.class.getName());

    private final WiggleClient client;
    private final String workerId;
    private final WorkerOptions options;
    private final Map<String, ActivityHandler> handlers = new ConcurrentHashMap<>();
    private final Map<String, NodeKind> kinds = new ConcurrentHashMap<>();
    private final Set<String> queues = ConcurrentHashMap.newKeySet();
    private final List<Blueprint<?>> blueprints = new CopyOnWriteArrayList<>();
    /** Compiled graphs by "name:version", for local-execution traversal. */
    private final Map<String, WorkflowDefinition> graphs = new ConcurrentHashMap<>();

    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicInteger inFlight = new AtomicInteger();
    private ExecutorService executor;
    private ScheduledExecutorService heartbeats;
    private Thread pollThread;

    private ThreadFactory heartbeatThreadFactory = new ThreadFactory() {
        private final AtomicInteger n = new AtomicInteger();
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "wiggle-heartbeat-" + workerId + "-" + n.incrementAndGet());
            t.setDaemon(true);
            return t;
        }
    };

    public Worker(WiggleClient client, String workerId) {
        this(client, workerId, WorkerOptions.defaults());
    }

    public Worker(WiggleClient client, String workerId, WorkerOptions options) {
        this.client = client;
        this.workerId = workerId;
        this.options = options;
    }

    public String workerId() { return workerId; }

    public int inFlight() { return inFlight.get(); }

    /** Adds a workflow's handlers to this worker's dispatch table. */
    public Worker register(Blueprint<?> blueprint) {
        blueprints.add(blueprint);
        handlers.putAll(blueprint.handlers());
        graphs.put(blueprint.definition().key(), blueprint.definition());
        blueprint.definition().nodes().values().stream()
                .filter(n -> n.isWorkerDispatched())
                .forEach(n -> {
                    kinds.put(n.activity(), n.kind());
                    if (n.queue() != null) queues.add(n.queue());
                });
        return this;
    }

    public Worker start() {
        if (!running.compareAndSet(false, true)) return this;
        if (options.registerOnStart()) {
            for (Blueprint<?> bp : blueprints) client.register(bp);
        }
        executor = Executors.newVirtualThreadPerTaskExecutor();
        heartbeats = Executors.newScheduledThreadPool(heartbeatThreads(), heartbeatThreadFactory);
        pollThread = new Thread(this::pollLoop, "wiggle-worker-" + workerId);
        pollThread.setDaemon(true);
        pollThread.start();
        LOG.log(System.Logger.Level.INFO, () -> "worker " + workerId + " polling queues " + queues
                + " with concurrency " + options.concurrency());
        return this;
    }

    private void pollLoop() {
        while (running.get()) {
            try {
                int free = options.concurrency() - inFlight.get();
                if (free <= 0) {
                    sleep(options.idleBackoff().toMillis());
                    continue;
                }
                List<TaskActivation> tasks = client.poll(workerId, queues, free,
                        options.lease().toMillis(), options.longPollWait().toMillis());
                if (tasks.isEmpty()) {
                    sleep(options.idleBackoff().toMillis());
                    continue;
                }
                for (TaskActivation task : tasks) {
                    inFlight.incrementAndGet();
                    executor.submit(() -> {
                        try {
                            execute(task);
                        } finally {
                            inFlight.decrementAndGet();
                        }
                    });
                }
            } catch (RuntimeException e) {
                if (!running.get()) return;
                LOG.log(System.Logger.Level.WARNING, "poll failed: " + e.getMessage());
                sleep(options.errorBackoff().toMillis());
            }
        }
    }

    private void execute(TaskActivation task) {
        WorkflowDefinition def = graphs.get(task.workflow() + ":" + task.version());
        // Local execution needs the graph to traverse; without it (unregistered version) fall back
        // to server-driven, one step at a time.
        if (task.executionMode() != ExecutionMode.SERVER && def != null) {
            executeLocal(task, def);
        } else {
            executeServer(task);
        }
    }

    /** Server-driven: run one step and report via complete/fail; the server advances the token. */
    private void executeServer(TaskActivation task) {
        ActivityHandler handler = handlers.get(task.activity());
        if (handler == null) {
            reportFailure(task, "no handler registered for activity '" + task.activity() + "'", false);
            return;
        }
        Heartbeat lease = new Heartbeat(heartbeats,
                extend -> client.heartbeat(task.taskId(), task.leaseOwner(), extend),
                options.lease().toMillis(), task.taskId());
        lease.start();
        Step.begin(new Step.Info(task.attempt(), task.stepName(), task.instanceId()));
        try {
            Object result = handler.invoke(task.context());
            lease.stop();
            if (task.kind() == NodeKind.PREDICATE && !(result instanceof Boolean)) {
                reportFailure(task, "predicate '" + task.stepName() + "' returned "
                        + (result == null ? "null" : result.getClass().getSimpleName()), false);
                return;
            }
            client.complete(task.taskId(), task.leaseOwner(),
                    task.kind() == NodeKind.PREDICATE ? Map.of("value", result) : result);
        } catch (PermanentActivityException e) {
            lease.stop();
            reportFailure(task, describe(e), false);
        } catch (Exception e) {
            LOG.log(System.Logger.Level.DEBUG,
                    () -> "step " + task.stepName() + " of " + task.instanceId() + " failed: " + e);
            lease.stop();
            reportFailure(task, describe(e), true);
        } catch (Throwable t) {
            lease.stop();
            reportFailure(task, describe(t), false);
            throw t;
        } finally {
            lease.stop();
            Step.end();
        }
    }

    /**
     * Local execution: run consecutive same-queue steps in-worker, reporting to the server until
     * the next node is a boundary (sleep / fork / join / user task / other queue / end) or the
     * instance is no longer running. LOCAL_SYNC flushes every step (batch size 1, one-step crash
     * blast radius); LOCAL_ASYNC buffers up to {@code localBatchSize} steps and flushes the run in
     * one call (fewer round-trips, whole-batch crash blast radius).
     *
     * <p>{@code serverTaskId} always names the token the server currently has leased to us -- the
     * node of the first un-flushed buffered step (or, when the buffer is empty, the next node to
     * run). A single lease guard heartbeats that token across the whole run.
     */
    private void executeLocal(TaskActivation task, WorkflowDefinition def) {
        int maxBatch = task.executionMode() == ExecutionMode.LOCAL_ASYNC ? options.localBatchSize() : 1;
        String leaseOwner = task.leaseOwner();
        String instanceId = task.instanceId();
        Node node = def.node(task.nodeId());
        Object ctx = task.context();
        int attempt = task.attempt();   // 1-based; continuation tokens are fresh (attempt 1)

        AtomicReference<String> serverTaskId = new AtomicReference<>(task.taskId());
        List<WiggleClient.StepReport> buffer = new ArrayList<>();
        Heartbeat lease = new Heartbeat(heartbeats,
                extend -> client.heartbeat(serverTaskId.get(), leaseOwner, extend),
                options.lease().toMillis(), task.taskId());
        lease.start();
        try {
            while (true) {
                ActivityHandler handler = handlers.get(node.activity());
                if (handler == null) {
                    flush(buffer, serverTaskId, leaseOwner);   // commit what succeeded
                    client.fail(serverTaskId.get(), leaseOwner,
                            "no handler registered for activity '" + node.activity() + "'", false);
                    return;
                }
                Object result;
                Step.begin(new Step.Info(attempt, node.name(), instanceId));
                try {
                    result = handler.invoke(ctx);
                } catch (PermanentActivityException e) {
                    Step.end();
                    if (flush(buffer, serverTaskId, leaseOwner)) client.fail(serverTaskId.get(), leaseOwner, describe(e), false);
                    return;
                } catch (Exception e) {
                    Step.end();
                    String stepName = node.name();
                    LOG.log(System.Logger.Level.DEBUG,
                            () -> "local step " + stepName + " of " + instanceId + " failed: " + e);
                    if (flush(buffer, serverTaskId, leaseOwner)) client.fail(serverTaskId.get(), leaseOwner, describe(e), true);
                    return;
                }
                Step.end();

                boolean isPredicate = node.kind() == NodeKind.PREDICATE;
                if (isPredicate && !(result instanceof Boolean)) {
                    if (flush(buffer, serverTaskId, leaseOwner)) client.fail(serverTaskId.get(), leaseOwner,
                            "predicate '" + node.name() + "' returned "
                                    + (result == null ? "null" : result.getClass().getSimpleName()), false);
                    return;
                }
                boolean predicateValue = isPredicate && (Boolean) result;
                Node next = def.node(GraphTraversal.successor(node, predicateValue));
                boolean handback = GraphTraversal.classify(next, queues) != null;

                buffer.add(isPredicate
                        ? new WiggleClient.StepReport(node.id(), null, predicateValue)
                        : new WiggleClient.StepReport(node.id(), result, null));
                if (!isPredicate) ctx = applyMerge(ctx, result);

                // A checkpoint forces a flush after this step even mid-chain (non-final), so it is
                // committed before the next runs -- SYNC already flushes every step, so this only
                // affects ASYNC.
                boolean checkpoint = def.checkpoints().contains(node.id());
                if (handback || checkpoint || buffer.size() >= maxBatch) {
                    AdvanceResult advanced = client.advanceRun(serverTaskId.get(), leaseOwner, buffer, handback);
                    buffer.clear();
                    if (!advanced.running() || handback || advanced.nextTaskId() == null) return;
                    serverTaskId.set(advanced.nextTaskId());
                }
                node = next;
                attempt = 1;
            }
        } finally {
            lease.stop();
        }
    }

    /**
     * Flushes buffered successful steps to the server (non-final), leaving {@code serverTaskId} at
     * the next node so a failure can be reported against the correct token.
     *
     * @return true if the instance is still running (safe to report a failure), false otherwise
     */
    private boolean flush(List<WiggleClient.StepReport> buffer, AtomicReference<String> serverTaskId, String leaseOwner) {
        if (buffer.isEmpty()) return true;
        AdvanceResult advanced = client.advanceRun(serverTaskId.get(), leaseOwner, List.copyOf(buffer), false);
        buffer.clear();
        if (advanced.nextTaskId() != null) serverTaskId.set(advanced.nextTaskId());
        return advanced.running();
    }

    /** Mirrors the server's context merge: a map result is shallow-merged; anything else replaces. */
    @SuppressWarnings("unchecked")
    private static Object applyMerge(Object ctx, Object result) {
        if (result == null) return ctx;
        if (!(result instanceof Map) || !(ctx instanceof Map)) return result;
        Map<String, Object> merged = new LinkedHashMap<>((Map<String, Object>) ctx);
        merged.putAll((Map<String, Object>) result);
        return merged;
    }

    /** A small pool: heartbeats are brief RPCs, so a handful of threads covers any concurrency. */
    private int heartbeatThreads() {
        return Math.max(1, Math.min(4, options.concurrency()));
    }

    private void reportFailure(TaskActivation task, String message, boolean retryable) {
        try {
            client.fail(task.taskId(), task.leaseOwner(), message, retryable);
        } catch (RuntimeException e) {
            // The lease will expire and the leader will reclaim the task; nothing else to do.
            LOG.log(System.Logger.Level.WARNING,
                    "could not report failure of task " + task.taskId() + ": " + e.getMessage());
        }
    }

    private static String describe(Throwable t) {
        String msg = t.getMessage();
        return t.getClass().getSimpleName() + (msg == null ? "" : ": " + msg);
    }

    private static void sleep(long millis) {
        if (millis <= 0) return;
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) return;
        if (pollThread != null) pollThread.interrupt();
        if (executor != null) {
            executor.shutdown();
            try {
                executor.awaitTermination(options.lease().toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (heartbeats != null) heartbeats.shutdownNow();
    }
}
