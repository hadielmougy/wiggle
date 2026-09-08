package com.wiggle.client.worker;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.dsl.ActivityHandler;
import com.wiggle.client.dsl.Blueprint;
import com.wiggle.core.*;

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
    private final List<Blueprint> blueprints = new CopyOnWriteArrayList<>();
    /** Compiled graphs by "name:version", for local-execution traversal. */
    private final Map<String, WorkflowDefinition> graphs = new ConcurrentHashMap<>();
    /** {@link Handlers @Handlers} objects, matched to graph steps by name on start. */
    private final List<HandlerSet> handlerSets = new CopyOnWriteArrayList<>();

    /** A registered {@link Handlers @Handlers} object: its step methods (by canonical name) and its
     *  {@link Decode @Decode} custom decoders (by decoded type). */
    private record HandlerSet(String workflow, Object target, Map<String, Method> byName,
                              Map<Class<?>, Method> decoders) {}

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
        if (handlerObject == null) throw new IllegalArgumentException("handlers object is required");
        Handlers ann = handlerObject.getClass().getAnnotation(Handlers.class);
        if (ann == null) {
            throw new IllegalArgumentException(handlerObject.getClass().getName()
                    + " is not annotated @Handlers(\"<workflow>\")");
        }
        String workflow = ann.value();
        if (workflow == null || workflow.isBlank()) {
            throw new IllegalArgumentException("@Handlers on " + handlerObject.getClass().getName()
                    + " needs the workflow name");
        }
        Map<String, Method> byName = new LinkedHashMap<>();
        Map<Class<?>, Method> decoders = new LinkedHashMap<>();
        for (Method m : handlerObject.getClass().getMethods()) {
            if (m.isSynthetic() || m.isBridge() || Modifier.isStatic(m.getModifiers())) continue;
            if (m.getDeclaringClass() == Object.class) continue;
            m.setAccessible(true);
            if (m.isAnnotationPresent(Decode.class)) {
                decoders.put(m.getReturnType(), m);
                continue;
            }
            if (m.getParameterCount() == 0) continue;   // a helper, not a handler
            String canon = canonicalName(m.getName());
            if (canon.isEmpty()) continue;
            Method prev = byName.putIfAbsent(canon, m);
            if (prev != null) {
                throw new IllegalArgumentException("methods '" + prev.getName() + "' and '" + m.getName()
                        + "' map to the same step name '" + canon + "'; names differing only in "
                        + "case/style are ambiguous -- rename one");
            }
        }
        handlerSets.add(new HandlerSet(workflow, handlerObject, byName, decoders));
        return this;
    }

    /** Decodes JSON into {@code type}: a class's {@link Decode @Decode} method if one is registered,
     *  otherwise a raw {@code Map} for Map types, else the record type via reflection. */
    private static Object decode(Object json, Class<?> type, Object target, Map<Class<?>, Method> decoders) throws Exception {
        Method dec = decoders.get(type);
        if (dec != null) return call(dec, target, Json.asObject(json));
        if (Map.class.isAssignableFrom(type)) return Json.asObject(json);
        return RecordMapper.fromJson(json, type);
    }

    /** Invokes a handler method, unwrapping the reflective exception to the real cause. */
    private static Object call(Method m, Object target, Object... args) throws Exception {
        try {
            return m.invoke(target, args);
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
        for (HandlerSet set : handlerSets) matchHandlerSet(set);
    }

    /**
     * Resolves a {@link Handlers @Handlers} object against the registered graph, node by node: each
     * worker-dispatched step is bound to the method whose name matches (case/style-insensitive), its
     * signature checked against the node's kind. A step (or combine) with no method is served by no
     * handler here (logged) — combines have no default fold, so one must be bound on some worker;
     * a method matching no step is a helper (ignored).
     */
    private void matchHandlerSet(HandlerSet set) {
        WorkflowDefinition def = fetchGraph(set.workflow());
        Set<String> served = new TreeSet<>();
        Set<String> allSteps = new TreeSet<>();
        for (Node node : def.nodes().values()) {
            if (!node.isWorkerDispatched() || node.name() == null) continue;
            allSteps.add(node.name());
            Method m = set.byName().get(canonicalName(node.name()));
            if (m == null) {
                // No handler on this worker for this step -- another worker may serve it. This
                // includes combine nodes: there is NO default fold; a combine served by no worker
                // fails its task at claim time ("no handler registered"), never merges implicitly.
                continue;
            }
            ActivityHandler handler = buildHandler(set, node, m);
            if (handlers.putIfAbsent(node.activity(), handler) != null) {
                throw new IllegalStateException("duplicate handler for activity '" + node.activity() + "'");
            }
            queues.add(node.queue() != null ? node.queue() : set.workflow());
            served.add(node.name());
        }
        graphs.put(def.key(), def);
        Set<String> unserved = new TreeSet<>(allSteps);
        unserved.removeAll(served);
        if (!unserved.isEmpty()) {   // info, not an error: this worker may intentionally serve a subset
            LOG.log(System.Logger.Level.INFO, () -> "workflow '" + set.workflow()
                    + "' has steps served by no handler on this worker: " + unserved);
        }
    }

    /** A combine node carries its fork arm names (a JSON array) on its itemsKey; a plain task does not. */
    private static boolean isCombine(Node node) {
        return node.kind() == NodeKind.TASK && node.itemsKey() != null;
    }

    private static List<String> armNames(Node node) {
        return Json.asArray(Json.parse(node.itemsKey())).stream().map(String::valueOf).toList();
    }

    /** Builds the handler for a graph node from its matched method, validating the signature vs kind. */
    private static ActivityHandler buildHandler(HandlerSet set, Node node, Method m) {
        Object target = set.target();
        Map<Class<?>, Method> decoders = set.decoders();
        if (isCombine(node)) return combineHandler(node, m, target, decoders);

        Class<?> ret = m.getReturnType();
        boolean returnsBool = (ret == boolean.class || ret == Boolean.class);
        Args args = splitArgs(node, m);   // one input parameter + an optional @Context parameter
        if (node.kind() == NodeKind.PREDICATE) {
            if (!returnsBool) {
                throw new IllegalStateException("gate '" + node.name() + "' handler '" + m.getName()
                        + "' must take the context and return boolean");
            }
            return ctx -> call(m, target, args.build(ctx, node, m, target, decoders));
        }
        if (returnsBool) {
            throw new IllegalStateException("step '" + node.name() + "' handler '" + m.getName()
                    + "' must take the context and return the next context (or void for an effect)");
        }
        boolean effect = (ret == void.class || ret == Void.class);
        if (effect) {
            return ctx -> { call(m, target, args.build(ctx, node, m, target, decoders)); return null; };
        }
        return ctx -> {
            // The return IS the next context: it is sent whole and REPLACES the previous value
            // server-side (no diff, no merge). A null return leaves the context untouched.
            Object out = call(m, target, args.build(ctx, node, m, target, decoders));
            return out == null ? null : RecordMapper.toJson(out);
        };
    }

    /** A handler's parameter layout: the input's position and type, plus an optional @Context slot.
     *  Either access style works — declare {@code @Context} to receive the frozen base as a
     *  parameter, or call {@code Step.base()} inside the method; both read the same value. */
    private record Args(int inputAt, Class<?> inputType, int contextAt, Class<?> contextType) {

        Object[] build(Object ctx, Node node, Method m, Object target, Map<Class<?>, Method> decoders)
                throws Exception {
            Object[] out = new Object[contextAt < 0 ? 1 : 2];
            out[inputAt] = decode(ctx, inputType, target, decoders);
            if (contextAt >= 0) {
                Map<String, Object> base;
                try {
                    base = Step.base();
                } catch (IllegalStateException e) {
                    throw new IllegalStateException("step '" + node.name() + "' handler '" + m.getName()
                            + "' declares a @Context parameter, but this step has no base context — "
                            + "@Context is only meaningful inside a forEach body (or a combine)", e);
                }
                out[contextAt] = decode(base, contextType, target, decoders);
            }
            return out;
        }
    }

    /** Splits a handler's parameters into the single input + an optional @Context parameter. */
    private static Args splitArgs(Node node, Method m) {
        java.lang.reflect.Parameter[] params = m.getParameters();
        int inputAt = -1;
        int contextAt = -1;
        for (int i = 0; i < params.length; i++) {
            if (params[i].isAnnotationPresent(Context.class)) {
                if (contextAt >= 0) inputAt = -2;   // two @Context params: invalid
                contextAt = i;
            } else if (inputAt == -1) {
                inputAt = i;
            } else {
                inputAt = -2;                        // two plain params: invalid
            }
        }
        if (inputAt < 0 || params.length > 2) {
            throw new IllegalStateException("step '" + node.name() + "' handler '" + m.getName()
                    + "' must take the input (plus at most one @Context parameter for the frozen base)");
        }
        return new Args(inputAt, params[inputAt].getType(), contextAt,
                contextAt < 0 ? null : params[contextAt].getType());
    }

    /**
     * A combine method; its return is the COMPLETE post-join context — the engine replaces the
     * context with it (nothing from before the join survives unless the handler returned it, and
     * staged scratch keys are stripped). Two flavors, told apart by the node's itemsKey:
     * <ul>
     *   <li><b>fork</b> (itemsKey = arm-name array): each {@link Arm @Arm} parameter gets that
     *       branch's final context decoded to its type; an optional {@link Context @Context}
     *       parameter gets the pre-fork context.</li>
     *   <li><b>forEach</b> (itemsKey = a scratch-key string): one collection parameter receives
     *       every item's final context — a {@code List} (ordered by item index) or {@code Set} for
     *       a list input, or a {@code Map} keyed like the input for a map input — with elements
     *       decoded to the collection's element type; an optional {@link Context @Context}
     *       parameter gets the pre-forEach context.</li>
     * </ul>
     */
    private static ActivityHandler combineHandler(Node node, Method m, Object target, Map<Class<?>, Method> decoders) {
        Object parsedKey = Json.parse(node.itemsKey());
        if (parsedKey instanceof String scratch) return forEachCombineHandler(node, m, target, decoders, scratch);
        List<String> arms = armNames(node);
        java.lang.reflect.Parameter[] params = m.getParameters();
        return ctx -> {
            Map<String, Object> map = Json.asObject(ctx);
            Map<String, Object> base = new LinkedHashMap<>(map);
            arms.forEach(base::remove);
            Object[] args = new Object[params.length];
            for (int i = 0; i < params.length; i++) {
                java.lang.reflect.Parameter p = params[i];
                Arm arm = p.getAnnotation(Arm.class);
                if (arm != null) {
                    args[i] = decode(map.get(arm.value()), p.getType(), target, decoders);
                } else if (p.isAnnotationPresent(Context.class)) {
                    args[i] = decode(base, p.getType(), target, decoders);
                } else {
                    throw new IllegalStateException("combine '" + node.name() + "' handler '" + m.getName()
                            + "' parameter " + i + " must be @Arm(\"branch\") or @Context");
                }
            }
            // Both access styles work: the @Context parameter above, or Step.base() inside the method.
            Object out = Step.withBase(base, () -> call(m, target, args));
            return out == null ? null : RecordMapper.toJson(out);
        };
    }

    /** The forEach flavor: bind the staged collection (list or map of item results) plus @Context. */
    private static ActivityHandler forEachCombineHandler(Node node, Method m, Object target,
                                                         Map<Class<?>, Method> decoders, String scratch) {
        java.lang.reflect.Parameter[] params = m.getParameters();
        return ctx -> {
            Map<String, Object> map = Json.asObject(ctx);
            Object staged = map.get(scratch);
            Object[] args = new Object[params.length];
            boolean itemsBound = false;
            for (int i = 0; i < params.length; i++) {
                java.lang.reflect.Parameter p = params[i];
                if (p.isAnnotationPresent(Context.class)) {
                    Map<String, Object> base = new LinkedHashMap<>(map);
                    base.remove(scratch);
                    args[i] = decode(base, p.getType(), target, decoders);
                } else if (!itemsBound) {
                    args[i] = decodeCollection(staged, p, target, decoders, node.name());
                    itemsBound = true;
                } else {
                    throw new IllegalStateException("forEach combine '" + node.name() + "' handler '"
                            + m.getName() + "' takes an optional @Context parameter and exactly one "
                            + "collection parameter (List/Set/Map) for the item results");
                }
            }
            if (!itemsBound) {
                throw new IllegalStateException("forEach combine '" + node.name() + "' handler '"
                        + m.getName() + "' needs a collection parameter (List/Set/Map) for the item results");
            }
            Map<String, Object> base = new LinkedHashMap<>(map);
            base.remove(scratch);
            // Both access styles work: a @Context parameter, or Step.base() inside the method.
            Object out = Step.withBase(base, () -> call(m, target, args));
            return out == null ? null : RecordMapper.toJson(out);
        };
    }

    /** Decodes the staged item results into the handler's declared collection type: a List keeps the
     *  item order, a Set deduplicates (LinkedHashSet, order-preserving), a Map is keyed like the
     *  input map. Elements decode to the collection's generic element type. */
    private static Object decodeCollection(Object staged, java.lang.reflect.Parameter p,
                                           Object target, Map<Class<?>, Method> decoders, String nodeName)
            throws Exception {
        Class<?> type = p.getType();
        Class<?> elem = elementType(p);
        if (Map.class.isAssignableFrom(type)) {
            Map<String, Object> out = new LinkedHashMap<>();
            if (staged instanceof Map<?, ?> mm) {
                for (Map.Entry<?, ?> e : mm.entrySet()) {
                    out.put(String.valueOf(e.getKey()), decode(e.getValue(), elem, target, decoders));
                }
            }
            return out;
        }
        List<Object> decoded = new java.util.ArrayList<>();
        if (staged instanceof List<?> list) {
            for (Object v : list) decoded.add(decode(v, elem, target, decoders));
        } else if (staged instanceof Map<?, ?> mm) {   // map input bound to a List/Set param: values, in key order
            for (Object v : mm.values()) decoded.add(decode(v, elem, target, decoders));
        }
        if (java.util.Set.class.isAssignableFrom(type)) return new java.util.LinkedHashSet<>(decoded);
        if (List.class.isAssignableFrom(type) || type == Object.class || type == java.util.Collection.class) {
            return decoded;
        }
        throw new IllegalStateException("forEach combine '" + nodeName + "': unsupported collection "
                + "parameter type " + type.getName() + " (use List, Set, or Map)");
    }

    /** The collection parameter's element type from its generics; Object (raw maps) when unknown. */
    private static Class<?> elementType(java.lang.reflect.Parameter p) {
        if (p.getParameterizedType() instanceof java.lang.reflect.ParameterizedType pt) {
            java.lang.reflect.Type[] args = pt.getActualTypeArguments();
            java.lang.reflect.Type t = args[args.length - 1];   // List<T>/Set<T> -> T; Map<K,V> -> V
            if (t instanceof Class<?> c) return c;
        }
        return Object.class;
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
