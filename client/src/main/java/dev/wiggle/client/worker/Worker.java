package dev.wiggle.client.worker;

import dev.wiggle.client.WiggleClient;
import dev.wiggle.client.dsl.ActivityHandler;
import dev.wiggle.client.dsl.Blueprint;
import dev.wiggle.core.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
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
    /** Handlers bound by name via {@link #handle}, reconciled against the server graph on start. */
    private final List<Claim> claims = new CopyOnWriteArrayList<>();
    /** Method objects bound via {@link #registerHandlers}, matched to graph steps by name on start. */
    private final List<HandlerSet> handlerSets = new CopyOnWriteArrayList<>();

    /** A {@link #handle}-bound step: the graph the server holds must agree on its name and kind. */
    private record Claim(String workflow, String step, NodeKind kind) {}

    /** One method of a {@link #registerHandlers} object: its expected kind and the ready-to-bind handler. */
    private record HandlerCandidate(String method, NodeKind kind, ActivityHandler handler) {}

    /** A {@link #registerHandlers} object's methods, keyed by canonical (case-folded) step name. */
    private record HandlerSet(String workflow, Map<String, HandlerCandidate> byCanonical) {}

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

    /** Whether the worker is currently running (started and not yet closed). */
    public boolean isRunning() { return running.get(); }

    /** Adds a workflow's handlers to this worker's dispatch table. */
    public Worker register(Blueprint<?> blueprint) {
        WorkflowDefinition def = blueprint.definition();
        blueprints.add(blueprint);
        handlers.putAll(blueprint.handlers());
        graphs.put(def.key(), def);
        queues.addAll(def.workerQueues());
        return this;
    }

    /**
     * Binds a handler to one step of an already-registered workflow, by name -- no topology
     * re-declaration. The graph lives on the server; this worker just implements {@code step}.
     * {@code fn} receives the context as JSON and returns the JSON object to merge back (only the
     * changed keys are sent); the merge is idempotent, so returning the whole context is fine too.
     * The queue the step polls is discovered from the graph on {@link #start()}, which also fails
     * fast if {@code step} does not exist in the workflow or is not a task.
     */
    public Worker handle(String workflow, String step, ActivityHandler fn) {
        return bind(workflow, step, NodeKind.TASK, ctx -> {
            Object out = fn.invoke(ctx);
            return out == null ? null : dev.wiggle.core.Json.shallowDiff(ctx, out);
        });
    }

    /**
     * Binds a predicate step (a gate / choose guard / do-while condition) by name; {@code fn} must
     * return a {@link Boolean}. See {@link #handle}.
     */
    public Worker handleGate(String workflow, String step, ActivityHandler fn) {
        return bind(workflow, step, NodeKind.PREDICATE, fn);
    }

    /**
     * Binds a side-effect step by name; {@code fn} consumes the context and the context is left
     * unchanged. See {@link #handle}.
     */
    public Worker handleEffect(String workflow, String step, java.util.function.Consumer<Object> fn) {
        return bind(workflow, step, NodeKind.TASK, ctx -> {
            fn.accept(ctx);
            return null;
        });
    }

    /**
     * Typed variant of {@link #handle(String, String, ActivityHandler)}: {@code fn} works on a
     * decoded {@code T} (e.g. a record via {@link ContextCodec#records}) and returns the new {@code T};
     * the codec encodes it and only the changed keys are sent back, exactly as the DSL's typed
     * {@code step} does. The wire form is the same JSON either way, so a typed Java handler and an
     * untyped ({@code dict}) Python handler interoperate on the same step.
     */
    public <T> Worker handle(String workflow, String step, ContextCodec<T> codec,
                             dev.wiggle.client.dsl.Activity<T> fn) {
        return bind(workflow, step, NodeKind.TASK, ctx -> {
            try {
                return Json.shallowDiff(ctx, codec.encode(fn.apply(codec.decode(ctx))));
            } finally {
                ContextVersion.clear();
            }
        });
    }

    /** Typed variant of {@link #handleGate}: {@code test} works on a decoded {@code T}. */
    public <T> Worker handleGate(String workflow, String step, ContextCodec<T> codec,
                                 dev.wiggle.client.dsl.Predicate<T> test) {
        return bind(workflow, step, NodeKind.PREDICATE, ctx -> {
            try {
                return test.test(codec.decode(ctx));
            } finally {
                ContextVersion.clear();
            }
        });
    }

    /** Typed variant of {@link #handleEffect}: {@code fn} consumes a decoded {@code T}. */
    public <T> Worker handleEffect(String workflow, String step, ContextCodec<T> codec,
                                   dev.wiggle.client.dsl.SideEffect<T> fn) {
        return bind(workflow, step, NodeKind.TASK, ctx -> {
            try {
                fn.accept(codec.decode(ctx));
                return null;
            } finally {
                ContextVersion.clear();
            }
        });
    }

    private Worker bind(String workflow, String step, NodeKind kind, ActivityHandler handler) {
        if (workflow == null || workflow.isBlank() || step == null || step.isBlank()) {
            throw new IllegalArgumentException("workflow and step are required");
        }
        String activity = workflow + "#" + step;
        if (handlers.putIfAbsent(activity, handler) != null) {
            throw new IllegalStateException("duplicate handler for activity '" + activity + "'");
        }
        claims.add(new Claim(workflow, step, kind));
        return this;
    }

    /**
     * Binds a whole object's methods as step handlers in one call. Each public method shaped like a
     * handler -- one {@code Map}/{@code Object} parameter (the context), returning the new context
     * (a {@code Map}, a task), a {@code boolean} (a gate), or {@code void} (a side effect) -- is
     * matched on {@link #start()} to a step of {@code workflow} by <b>case-insensitive name</b>
     * ({@code inStock} matches a step named {@code in-stock}), the graph confirming the exact name and
     * whether it is a gate. Methods of any other shape are ignored, so helpers can live on the object.
     *
     * <p>Two methods whose names collide under case-folding are rejected here (ambiguous); a method
     * matching no step, or a kind that clashes with the graph, is caught on {@link #start()}.
     */
    public Worker registerHandlers(String workflow, Object handlers) {
        if (workflow == null || workflow.isBlank()) {
            throw new IllegalArgumentException("workflow is required");
        }
        if (handlers == null) {
            throw new IllegalArgumentException("handlers is required");
        }
        Map<String, HandlerCandidate> byCanonical = new LinkedHashMap<>();
        for (Method m : handlers.getClass().getMethods()) {
            HandlerCandidate cand = candidateFor(handlers, m);
            if (cand == null) continue;
            String canon = canonicalName(m.getName());
            if (canon.isEmpty()) continue;
            HandlerCandidate prev = byCanonical.putIfAbsent(canon, cand);
            if (prev != null) {
                throw new IllegalArgumentException("methods '" + prev.method() + "' and '" + m.getName()
                        + "' map to the same step name '" + canon + "'; names differing only in "
                        + "case/style are ambiguous -- rename one");
            }
        }
        if (byCanonical.isEmpty()) {
            throw new IllegalArgumentException(handlers.getClass().getName() + " exposes no handler "
                    + "methods (a method taking the context and returning a Map (task), boolean (gate), "
                    + "or void (effect))");
        }
        handlerSets.add(new HandlerSet(workflow, byCanonical));
        return this;
    }

    /** Wraps a handler-shaped method as an {@link ActivityHandler}, or returns null if it is not one. */
    private static HandlerCandidate candidateFor(Object target, Method m) {
        if (m.isSynthetic() || m.isBridge() || Modifier.isStatic(m.getModifiers())) return null;
        if (m.getDeclaringClass() == Object.class) return null;
        if (m.getParameterCount() != 1) return null;
        // the context arrives as a Map (JSON object); accept a parameter a Map can be passed to
        if (!m.getParameterTypes()[0].isAssignableFrom(Map.class)) return null;
        m.setAccessible(true);
        Class<?> ret = m.getReturnType();
        if (ret == boolean.class || ret == Boolean.class) {
            return new HandlerCandidate(m.getName(), NodeKind.PREDICATE, ctx -> invoke(m, target, ctx));
        }
        if (ret == void.class || ret == Void.class) {
            return new HandlerCandidate(m.getName(), NodeKind.TASK, ctx -> {
                invoke(m, target, ctx);
                return null;
            });
        }
        return new HandlerCandidate(m.getName(), NodeKind.TASK, ctx -> {
            Object out = invoke(m, target, ctx);
            return out == null ? null : Json.shallowDiff(ctx, out);
        });
    }

    /** Invokes a bound handler method, unwrapping the reflective exception to the real cause. */
    private static Object invoke(Method m, Object target, Object ctx) throws Exception {
        try {
            return m.invoke(target, ctx);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception ex) throw ex;   // preserves PermanentActivityException etc.
            if (cause instanceof Error err) throw err;
            throw e;
        }
    }

    /**
     * Folds a name to a case/style-independent key: its lowercase alphanumerics, in order. So
     * {@code in-stock}, {@code in_stock}, {@code inStock}, {@code InStock}, and {@code instock} all
     * yield {@code instock}.
     */
    static String canonicalName(String s) {
        StringBuilder b = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            if (Character.isLetterOrDigit(ch)) b.append(Character.toLowerCase(ch));
        }
        return b.toString();
    }

    public Worker start() {
        if (!running.compareAndSet(false, true)) return this;
        if (options.registerOnStart()) {
            for (Blueprint<?> bp : blueprints) client.register(bp);
        }
        if (!claims.isEmpty() || !handlerSets.isEmpty()) reconcile();
        executor = Executors.newVirtualThreadPerTaskExecutor();
        heartbeats = Executors.newScheduledThreadPool(heartbeatThreads(), heartbeatThreadFactory);
        pollThread = new Thread(this::pollLoop, "wiggle-worker-" + workerId);
        pollThread.setDaemon(true);
        pollThread.start();
        LOG.log(System.Logger.Level.INFO, () -> "worker " + workerId + " polling queues " + servedQueues()
                + " with concurrency " + options.concurrency());
        return this;
    }

    /**
     * Checks every {@link #handle}-bound claim against the server's registered graph and learns the
     * queue each claimed step polls -- a name-only binding has no other way to know it. Fails fast on
     * a mistyped step name or a kind mismatch, so a bad binding surfaces at start rather than as a
     * silent runtime "no handler" much later.
     */
    private void reconcile() {
        Map<String, List<Claim>> byWorkflow = new LinkedHashMap<>();
        for (Claim c : claims) byWorkflow.computeIfAbsent(c.workflow(), k -> new ArrayList<>()).add(c);

        for (var entry : byWorkflow.entrySet()) {
            String wf = entry.getKey();
            WorkflowDefinition def = fetchGraph(wf);
            Map<String, Node> byActivity = new HashMap<>();
            for (Node n : def.nodes().values()) {
                if (n.isWorkerDispatched() && n.activity() != null) byActivity.put(n.activity(), n);
            }
            Set<String> served = new HashSet<>();
            for (Claim c : entry.getValue()) {
                String activity = wf + "#" + c.step();
                Node node = byActivity.get(activity);
                if (node == null) {
                    throw new IllegalStateException("no step '" + c.step() + "' in registered workflow '"
                            + wf + "' (available steps: " + availableSteps(byActivity) + ")");
                }
                if (node.kind() != c.kind()) {
                    String verb = node.kind() == NodeKind.PREDICATE ? "handleGate" : "handle";
                    throw new IllegalStateException("activity '" + activity + "' is a " + node.kind()
                            + " in the graph but was bound as " + c.kind() + "; use " + verb + "() instead");
                }
                queues.add(node.queue() != null ? node.queue() : wf);
                served.add(activity);
            }
            graphs.put(def.key(), def);
            Set<String> unclaimed = new TreeSet<>();
            for (String a : byActivity.keySet()) {
                if (!served.contains(a)) unclaimed.add(a.substring(a.indexOf('#') + 1));
            }
            if (!unclaimed.isEmpty()) {   // info, not an error: this worker may intentionally serve a subset
                LOG.log(System.Logger.Level.INFO,
                        () -> "workflow '" + wf + "' has steps served by no handler on this worker: " + unclaimed);
            }
        }
        for (HandlerSet set : handlerSets) matchHandlerSet(set);
    }

    /**
     * Resolves a {@link #registerHandlers} object against the registered graph: each method is matched
     * to a step by canonical name, its kind checked against the node, and its handler bound. A method
     * matching no step (or a kind clash) fails fast, exactly like a mistyped {@link #handle}.
     */
    private void matchHandlerSet(HandlerSet set) {
        WorkflowDefinition def = fetchGraph(set.workflow());
        Map<String, Node> nodeByCanonical = new HashMap<>();
        Map<String, String> stepByCanonical = new HashMap<>();   // canonical -> real step name
        for (Node n : def.nodes().values()) {
            if (!n.isWorkerDispatched() || n.name() == null) continue;
            String c = canonicalName(n.name());
            nodeByCanonical.putIfAbsent(c, n);
            stepByCanonical.putIfAbsent(c, n.name());
        }
        Set<String> served = new TreeSet<>();
        for (var e : set.byCanonical().entrySet()) {
            HandlerCandidate cand = e.getValue();
            Node node = nodeByCanonical.get(e.getKey());
            if (node == null) {
                throw new IllegalStateException("handler '" + cand.method() + "' matches no step in "
                        + "workflow '" + set.workflow() + "' (available steps: "
                        + new TreeSet<>(stepByCanonical.values()) + ")");
            }
            String step = stepByCanonical.get(e.getKey());
            String activity = set.workflow() + "#" + step;
            if (node.kind() != cand.kind()) {
                throw new IllegalStateException("activity '" + activity + "' is a " + node.kind()
                        + " in the graph but handler '" + cand.method() + "' is a " + cand.kind());
            }
            if (handlers.putIfAbsent(activity, cand.handler()) != null) {
                throw new IllegalStateException("duplicate handler for activity '" + activity + "'");
            }
            queues.add(node.queue() != null ? node.queue() : set.workflow());
            served.add(step);
        }
        graphs.put(def.key(), def);
        Set<String> unclaimed = new TreeSet<>();
        for (String s : stepByCanonical.values()) {
            if (!served.contains(s)) unclaimed.add(s);
        }
        if (!unclaimed.isEmpty()) {   // info, not an error: this worker may intentionally serve a subset
            LOG.log(System.Logger.Level.INFO, () -> "workflow '" + set.workflow()
                    + "' has steps served by no handler on this worker: " + unclaimed);
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

    private static java.util.List<String> availableSteps(Map<String, Node> byActivity) {
        java.util.List<String> steps = new ArrayList<>();
        for (String a : byActivity.keySet()) steps.add(a.substring(a.indexOf('#') + 1));
        Collections.sort(steps);
        return steps;
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
        PollResult result = client.poll(workerId, servedQueues(), free,
                options.lease().toMillis(), options.longPollWait().toMillis());
        List<TaskActivation> tasks = result.tasks();
        if (tasks.isEmpty()) {
            // The server's hold-off hint (set when it is shedding under memory pressure) takes
            // precedence over the normal idle backoff.
            long backoff = result.retryAfterMillis() > 0 ? result.retryAfterMillis()
                    : options.idleBackoff().toMillis();
            LOG.log(System.Logger.Level.WARNING,"pool request is rejected by server: [backoff:" + backoff+"]");
            sleep(backoff);
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
