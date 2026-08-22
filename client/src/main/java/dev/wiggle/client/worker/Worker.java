package dev.wiggle.client.worker;

import dev.wiggle.client.dsl.ActivityHandler;
import dev.wiggle.client.dsl.Blueprint;
import dev.wiggle.core.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The data plane. A worker registers its blueprints, then pulls work: it only ever
 * asks for as many tasks as it has free slots, so the server never overwhelms it and
 * backpressure is a property of the protocol rather than a thing to configure.
 *
 * Workers hold no durable state -- nothing survives a crash, and nothing needs to,
 * since every commit lands on the server. A crash loses at most the current step
 * (SERVER/LOCAL_SYNC) or the current local-execution batch (LOCAL_ASYNC); either way
 * the leader reclaims the lease and it re-runs. A graceful {@link #close()} does
 * better: it drains any buffered LOCAL_ASYNC steps to the server before exiting, so an
 * orderly shutdown (a rolling deploy, a scale-down) does not even pay that replay cost.
 */
public final class Worker implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(Worker.class.getName());

    private final WiggleClient client;
    private final String workerId;
    private final WorkerOptions options;
    private final Map<String, ActivityHandler> handlers = new ConcurrentHashMap<>();
    private final Set<String> queues = ConcurrentHashMap.newKeySet();
    private final List<Blueprint<?>> blueprints = new CopyOnWriteArrayList<>();
    /** Compiled graphs by "name:version", for local-execution traversal. */
    private final Map<String, WorkflowDefinition> graphs = new ConcurrentHashMap<>();

    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicInteger inFlight = new AtomicInteger();
    private ExecutorService executor;
    private ScheduledExecutorService heartbeats;
    private Thread pollThread;

    private final ThreadFactory heartbeatThreadFactory = new ThreadFactory() {
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

    /** The queues this worker actually serves: the explicit restriction, or everything registered. */
    private Set<String> servedQueues() {
        return options.queues().isEmpty() ? queues : options.queues();
    }

    public int inFlight() { return inFlight.get(); }

    /** Adds a workflow's handlers to this worker's dispatch table. */
    public Worker register(Blueprint<?> blueprint) {
        WorkflowDefinition def = blueprint.definition();
        blueprints.add(blueprint);
        handlers.putAll(blueprint.handlers());
        graphs.put(def.key(), def);
        queues.addAll(def.workerQueues());
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
        LOG.log(System.Logger.Level.INFO, () -> "worker " + workerId + " polling queues " + servedQueues()
                + " with concurrency " + options.concurrency());
        return this;
    }

    private void pollLoop() {
        while (running.get()) {
            try {
                pollOnce();
            } catch (RuntimeException e) {
                if (!running.get()) return;
                LOG.log(System.Logger.Level.WARNING, "poll failed: " + e.getMessage());
                sleep(options.errorBackoff().toMillis());
            }
        }
    }

    private void pollOnce() {
        int free = options.concurrency() - inFlight.get();
        if (free <= 0) {
            sleep(options.idleBackoff().toMillis());
            return;
        }
        List<TaskActivation> tasks = client.poll(workerId, servedQueues(), free,
                options.lease().toMillis(), options.longPollWait().toMillis());
        if (tasks.isEmpty()) {
            sleep(options.idleBackoff().toMillis());
            return;
        }
        for (TaskActivation task : tasks) {
            submit(task);
        }
    }

    private void submit(TaskActivation task) {
        inFlight.incrementAndGet();
        executor.submit(() -> {
            try {
                execute(task);
            } finally {
                inFlight.decrementAndGet();
            }
        });
    }

    private void execute(TaskActivation task) {
        WorkflowDefinition def = graphs.get(task.workflow() + ":" + task.version());
        // Local execution needs the graph to traverse; without it (unregistered version) fall back
        // to server-driven, one step at a time.
        if (task.executionMode() != ExecutionMode.SERVER && def != null) {
            new LocalRun(task, def).run();
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
        Heartbeat lease = newHeartbeat(task.taskId(), task.leaseOwner());
        lease.start();
        Step.begin(new Step.Info(task.attempt(), task.stepName(), task.instanceId()));
        try {
            Object result = handler.invoke(task.context());
            lease.stop();   // the handler is done: no extension may race or trail the settle below
            settle(task, result);
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

    /** Reports a finished step: a predicate must have produced a boolean, a task merges its result. */
    private void settle(TaskActivation task, Object result) {
        if (task.kind() == NodeKind.PREDICATE && !(result instanceof Boolean)) {
            reportFailure(task, "predicate '" + task.stepName() + "' returned " + typeName(result), false);
            return;
        }
        client.complete(task.taskId(), task.leaseOwner(),
                task.kind() == NodeKind.PREDICATE ? Map.of("value", result) : result);
    }

    private Heartbeat newHeartbeat(String taskId, String leaseOwner) {
        return new Heartbeat(heartbeats,
                extend -> client.heartbeat(taskId, leaseOwner, extend),
                options.lease().toMillis(), taskId);
    }

    /**
     * One local execution run (LOCAL_SYNC / LOCAL_ASYNC): consecutive same-queue steps executed
     * in-worker until the next node is a boundary (sleep / fork / join / user task / other queue /
     * end) or the instance stops running. LOCAL_SYNC flushes every step (one-step crash blast
     * radius); LOCAL_ASYNC buffers up to {@code localBatchSize} steps and flushes the run in one
     * call. Owning the chain state here keeps each step's logic flat.
     */
    private final class LocalRun {
        private final WorkflowDefinition def;
        private final String leaseOwner;
        private final String instanceId;
        private final int maxBatch;
        private final List<WiggleClient.StepReport> buffer = new ArrayList<>();
        /** The token the server currently has leased to us; read by the heartbeat thread. */
        private volatile String serverTaskId;
        private Node node;
        private Object ctx;
        private int attempt;

        LocalRun(TaskActivation task, WorkflowDefinition def) {
            this.def = def;
            this.leaseOwner = task.leaseOwner();
            this.instanceId = task.instanceId();
            this.maxBatch = task.executionMode() == ExecutionMode.LOCAL_ASYNC ? options.localBatchSize() : 1;
            this.serverTaskId = task.taskId();
            this.node = def.node(task.nodeId());
            this.ctx = task.context();
            this.attempt = task.attempt();   // 1-based; continuation tokens are fresh (attempt 1)
        }

        void run() {
            Heartbeat lease = new Heartbeat(heartbeats,
                    extend -> client.heartbeat(serverTaskId, leaseOwner, extend),
                    options.lease().toMillis(), serverTaskId);
            lease.start();
            try {
                boolean chaining = true;
                // Re-checked between steps (never mid-handler), so a shutdown drains promptly:
                // the step already in flight finishes normally, then the loop stops here instead
                // of picking up another one.
                while (chaining && running.get()) {
                    chaining = runOneStep();
                }
                if (chaining) drainOnShutdown();
            } finally {
                lease.stop();
            }
        }

        /**
         * Called when the worker is closing while steps remain buffered or a further step could
         * still run locally. Flushes what's already been computed -- so it survives the
         * restart instead of being silently discarded -- and forces a handback (even though the
         * next node may itself be locally runnable) so the continuation is immediately READY for
         * another worker rather than sitting leased to one that is shutting down.
         */
        private void drainOnShutdown() {
            if (buffer.isEmpty()) return;   // nothing computed yet; the claimed lease simply expires and is reclaimed
            try {
                client.advanceRun(serverTaskId, leaseOwner, List.copyOf(buffer), true);
                int drained = buffer.size();
                buffer.clear();
                LOG.log(System.Logger.Level.DEBUG, () -> "drained " + drained
                        + " buffered step(s) of instance " + instanceId + " on shutdown");
            } catch (RuntimeException e) {
                // Best effort: the lease will simply expire and the leader will reclaim it,
                // re-running from the last successful flush -- the same guarantee a crash gives.
                LOG.log(System.Logger.Level.WARNING,
                        "could not drain buffered steps of instance " + instanceId + " on shutdown: " + e);
            }
        }

        /** Executes the current node; true = keep chaining locally. */
        private boolean runOneStep() {
            ActivityHandler handler = handlers.get(node.activity());
            if (handler == null) {
                failRun("no handler registered for activity '" + node.activity() + "'", false);
                return false;
            }
            Invocation outcome = invoke(handler);
            if (!outcome.ok()) return false;
            if (node.kind() == NodeKind.PREDICATE && !(outcome.result() instanceof Boolean)) {
                failRun("predicate '" + node.name() + "' returned " + typeName(outcome.result()), false);
                return false;
            }
            return advance(outcome.result());
        }

        private Invocation invoke(ActivityHandler handler) {
            Step.begin(new Step.Info(attempt, node.name(), instanceId));
            try {
                return Invocation.ok(handler.invoke(ctx));
            } catch (PermanentActivityException e) {
                failRun(describe(e), false);
                return Invocation.failed();
            } catch (Exception e) {
                String stepName = node.name();
                LOG.log(System.Logger.Level.DEBUG,
                        () -> "local step " + stepName + " of " + instanceId + " failed: " + e);
                failRun(describe(e), true);
                return Invocation.failed();
            } finally {
                Step.end();
            }
        }

        /** Records the step, flushes when due, and moves to the successor; false = run is over. */
        private boolean advance(Object result) {
            boolean isPredicate = node.kind() == NodeKind.PREDICATE;
            boolean predicateValue = isPredicate && (Boolean) result;
            Node next = def.node(GraphTraversal.successor(node, predicateValue));
            boolean handback = GraphTraversal.classify(next, servedQueues()) != null;
            buffer.add(isPredicate
                    ? new WiggleClient.StepReport(node.id(), null, predicateValue)
                    : new WiggleClient.StepReport(node.id(), result, null));
            if (!isPredicate) ctx = applyMerge(ctx, result);
            if (shouldFlush(handback) && !flushAndContinue(handback)) return false;
            node = next;
            attempt = 1;
            return true;
        }

        /**
         * A boundary or a full buffer always flushes; so does a checkpoint, which commits its step
         * before the next runs even mid-chain (SYNC already flushes every step, so this only
         * affects ASYNC).
         */
        private boolean shouldFlush(boolean handback) {
            return handback || def.checkpoints().contains(node.id()) || buffer.size() >= maxBatch;
        }

        /** Flushes the buffer; true = the server leased us the continuation, keep chaining. */
        private boolean flushAndContinue(boolean handback) {
            AdvanceResult advanced = client.advanceRun(serverTaskId, leaseOwner, List.copyOf(buffer), handback);
            buffer.clear();
            if (!advanced.running() || handback || advanced.nextTaskId() == null) return false;
            serverTaskId = advanced.nextTaskId();
            return true;
        }

        /**
         * Commits any buffered successful steps (leaving {@code serverTaskId} at the failing node's
         * token), then reports the failure -- unless the instance already stopped running.
         */
        private void failRun(String message, boolean retryable) {
            if (!flushBeforeFailure()) return;
            client.fail(serverTaskId, leaseOwner, message, retryable);
        }

        /** @return true if the instance is still running (safe to report a failure) */
        private boolean flushBeforeFailure() {
            if (buffer.isEmpty()) return true;
            AdvanceResult advanced = client.advanceRun(serverTaskId, leaseOwner, List.copyOf(buffer), false);
            buffer.clear();
            if (advanced.nextTaskId() != null) serverTaskId = advanced.nextTaskId();
            return advanced.running();
        }
    }

    /** The outcome of invoking a handler: a result, or "already reported as failed". */
    private record Invocation(boolean ok, Object result) {
        static Invocation ok(Object result) { return new Invocation(true, result); }
        static Invocation failed() { return new Invocation(false, null); }
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

    private static String typeName(Object result) {
        return result == null ? "null" : result.getClass().getSimpleName();
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

    /**
     * Stops polling and lets in-flight steps finish. Flips {@link #running} first, so any
     * {@link LocalRun} in progress sees it at its next between-steps check and drains instead of
     * continuing to chain (see {@link LocalRun#drainOnShutdown()}) -- a graceful shutdown loses
     * nothing already computed. The {@code executor.shutdown()} below does not interrupt the
     * current step; it only stops new submissions, so that in-flight step (and, for a local run,
     * its drain flush) gets to complete.
     */
    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) return;
        if (pollThread != null) pollThread.interrupt();
        awaitExecutor();
        if (heartbeats != null) heartbeats.shutdownNow();
    }

    private void awaitExecutor() {
        if (executor == null) return;
        executor.shutdown();
        try {
            // Bounded by one step plus one flush RPC now that a local run drains rather than
            // chaining to a natural boundary, so the lease duration is ample headroom.
            executor.awaitTermination(options.lease().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
