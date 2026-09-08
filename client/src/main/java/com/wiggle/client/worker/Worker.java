package com.wiggle.client.worker;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.dsl.ActivityHandler;
import com.wiggle.client.dsl.Blueprint;
import com.wiggle.core.*;

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
    private final List<Blueprint> blueprints = new CopyOnWriteArrayList<>();
    /** Compiled graphs by "name:version", for local-execution traversal. */
    private final Map<String, WorkflowDefinition> graphs = new ConcurrentHashMap<>();
    /** {@link Handlers @Handlers} objects, matched to graph steps by name on start. */
    private final List<HandlerBinder.HandlerSet> handlerSets = new CopyOnWriteArrayList<>();


    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicInteger inFlight = new AtomicInteger();
    /** Signalled whenever a task finishes, so a saturated poll loop resumes the moment capacity frees
     *  instead of napping a fixed idle-backoff (which used to gate throughput to one wave per nap). */
    private final Object capacityFreed = new Object();
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

    /** Whether the worker is currently running (started and not yet closed). */
    public boolean isRunning() { return running.get(); }

    /** Registers a workflow's topology on this worker (the graph it will poll and drive). */
    public Worker register(Blueprint blueprint) {
        WorkflowDefinition def = blueprint.definition();
        blueprints.add(blueprint);
        graphs.put(def.key(), def);
        queues.addAll(def.workerQueues());
        return this;
    }

    /**
     * Binds a {@link Handlers @Handlers}-annotated object's methods as this worker's step
     * implementations. The annotation names the workflow; each method whose name matches a step
     * (case/style-insensitive, so {@code inStock} binds {@code in-stock}) is a handler, its signature
     * defining the step: one parameter is the input (decoded from JSON into that type), a
     * {@code boolean} return is a gate, {@code void} an effect, any other return type a task whose
     * value becomes the next context. A method with {@link Arm @Arm} parameters is the combine for the
     * matching {@code combine} node -- each branch's result decoded into its parameter's type, plus an
     * optional {@link Context @Context} parameter for the pre-fork context. A {@link Decode @Decode}
     * method is a custom decoder for its return type (versioning / upcasts / bespoke codecs).
     *
     * <p>Matched against the registered graph on {@link #start()}: a name collision here, or a
     * signature that clashes with the graph node's kind, fails fast; a step with no matching method is
     * simply served by no handler on this worker (logged). This includes combine nodes: there is no
     * default fold — every combine must have an explicit handler on some worker, and its return is
     * the complete post-join context.
     */
    public Worker handlers(Object handlerObject) {
        handlerSets.add(HandlerBinder.scan(handlerObject));
        return this;
    }

    public Worker start() {
        if (!running.compareAndSet(false, true)) return this;
        if (options.registerOnStart()) {
            for (Blueprint bp : blueprints) client.register(bp);
        }
        if (!handlerSets.isEmpty()) reconcile();
        executor = Executors.newVirtualThreadPerTaskExecutor();
        heartbeats = Executors.newScheduledThreadPool(heartbeatThreads(), heartbeatThreadFactory);
        pollThread = new Thread(this::pollLoop, "wiggle-worker-" + workerId);
        pollThread.setDaemon(true);
        pollThread.start();
        LOG.log(System.Logger.Level.INFO, () -> "worker " + workerId + " polling queues " + servedQueues()
                + " with concurrency " + options.concurrency());
        return this;
    }

    private void reconcile() {
        for (HandlerBinder.HandlerSet set : handlerSets) matchHandlerSet(set);
    }

    /**
     * Resolves a {@link Handlers @Handlers} object against the registered graph (fetched here — the
     * binder itself is pure) and installs the resulting bindings. See {@link HandlerBinder}.
     */
    private void matchHandlerSet(HandlerBinder.HandlerSet set) {
        WorkflowDefinition def = fetchGraph(set.workflow());
        HandlerBinder.Result result = HandlerBinder.bind(set, def);
        for (HandlerBinder.Binding b : result.bindings()) {
            if (handlers.putIfAbsent(b.activity(), b.handler()) != null) {
                throw new IllegalStateException("duplicate handler for activity '" + b.activity() + "'");
            }
            queues.add(b.queue());
        }
        graphs.put(def.key(), def);
        if (!result.unserved().isEmpty()) {   // info, not an error: this worker may serve a subset
            LOG.log(System.Logger.Level.INFO, () -> "workflow '" + set.workflow()
                    + "' has steps served by no handler on this worker: " + result.unserved());
        }
    }

    /** Fetches the registered graph, waiting out a registration race up to {@code awaitRegistration}. */
    private WorkflowDefinition fetchGraph(String workflow) {
        long deadline = System.nanoTime() + options.awaitRegistration().toNanos();
        while (true) {
            try {
                return client.getWorkflow(workflow);
            } catch (WiggleClient.WiggleApiException e) {
                boolean notFound = e.status() == 404;
                if (notFound && System.nanoTime() < deadline) {
                    sleep(250);
                    continue;
                }
                if (notFound) {
                    throw new IllegalStateException("workflow '" + workflow + "' is not registered; register "
                            + "its graph before starting a worker that binds handlers to it (or set "
                            + "WorkerOptions.withAwaitRegistration)", e);
                }
                throw e;
            }
        }
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
        if (options.concurrency() - inFlight.get() <= 0) {
            // Saturated: wait for a task to finish (signalled below), not a fixed nap. A fixed nap
            // gated throughput to one concurrency-sized wave per idle-backoff, because fast steps all
            // finished while the poll thread was still asleep.
            awaitCapacity(options.idleBackoff().toMillis());
            return;
        }
        int free = options.concurrency() - inFlight.get();
        long polledAt = System.currentTimeMillis();
        PollResult result = client.poll(workerId, servedQueues(), free,
                options.lease().toMillis(), options.longPollWait().toMillis());
        List<TaskActivation> tasks = result.tasks();
        if (tasks.isEmpty()) {
            long shedFor = result.retryAfterMillis();
            if (shedFor > 0) {
                // The server is shedding under load (memory pressure); honour its hold-off hint.
                LOG.log(System.Logger.Level.WARNING, "poll shed by server under load; backing off " + shedFor + "ms");
                sleep(shedFor);
            } else if (System.currentTimeMillis() - polledAt < MIN_LONG_POLL_MILLIS) {
                // The server answered instantly instead of holding the long-poll open (old server, or
                // waitMillis clamped to 0): back off so an idle worker doesn't spin.
                LOG.log(System.Logger.Level.DEBUG, () -> "empty short poll; idle backoff");
                sleep(options.idleBackoff().toMillis());
            }
            // else: the server held the long-poll to its deadline and found nothing -- re-poll
            // immediately; the server-side long-poll (with wake-on-produce) IS the idle wait.
            return;
        }
        for (TaskActivation task : tasks) {
            submit(task);
        }
    }

    /** An empty poll that returns faster than this didn't long-poll server-side; back off client-side. */
    private static final long MIN_LONG_POLL_MILLIS = 5;

    /** Parks until a task slot frees (or {@code maxWaitMillis} as a safety net). */
    private void awaitCapacity(long maxWaitMillis) {
        long deadline = System.currentTimeMillis() + maxWaitMillis;
        synchronized (capacityFreed) {
            while (running.get() && options.concurrency() - inFlight.get() <= 0) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) return;
                try {
                    capacityFreed.wait(remaining);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private void submit(TaskActivation task) {
        inFlight.incrementAndGet();
        executor.submit(() -> {
            try {
                execute(task);
            } finally {
                inFlight.decrementAndGet();
                synchronized (capacityFreed) {
                    capacityFreed.notifyAll();   // resume a saturated poll loop immediately
                }
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
        Step.begin(new Step.Info(task.attempt(), task.stepName(), task.instanceId(),
                task.baseContext(), task.baseContext() != null, task.itemIndex(), task.itemMapKey()));
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
        /** forEach item scope, frozen for the whole local chain (null outside an item body). */
        private final Object baseContext;
        private final long itemIndex;
        private final String itemMapKey;
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
            this.baseContext = task.baseContext();
            this.itemIndex = task.itemIndex();
            this.itemMapKey = task.itemMapKey();
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
            Step.begin(new Step.Info(attempt, node.name(), instanceId,
                    baseContext, baseContext != null, itemIndex, itemMapKey));
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
            if (!isPredicate) ctx = applyReplace(ctx, result);
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

    /** Mirrors the server: a step's return REPLACES the context (null = unchanged, no merge). */
    private static Object applyReplace(Object ctx, Object result) {
        return result == null ? ctx : result;
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
