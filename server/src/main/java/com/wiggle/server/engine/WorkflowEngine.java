package com.wiggle.server.engine;

import com.wiggle.core.ExecutionMode;
import com.wiggle.core.Ids;
import com.wiggle.core.InstanceView;
import com.wiggle.core.Json;
import com.wiggle.core.Node;
import com.wiggle.core.NodeKind;
import com.wiggle.core.RetryPolicy;
import com.wiggle.core.ScratchKeys;
import com.wiggle.core.TaskActivation;
import com.wiggle.core.WorkflowDefinition;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.Rows.InstanceStatus;
import com.wiggle.server.store.Rows.Token;
import com.wiggle.server.store.Rows.TokenStatus;
import com.wiggle.server.store.Storage;
import com.wiggle.server.store.Tx;
import java.util.*;

/**
 * The state machine. Everything an instance does is expressed as tokens moving over
 * the compiled graph, in the spirit of a Petri net: a fork mints one token per branch,
 * a join consumes them, and the instance is terminal when no token is active.
 *
 * All mutations for a given instance are serialised by {@link Tx#lockInstance}, which
 * is what lets several server nodes drive the same instance without stepping on
 * each other.
 */
public final class WorkflowEngine {

    private static final System.Logger LOG = System.getLogger(WorkflowEngine.class.getName());

    /** How long a long-poll waits between fallback DB claims when no local wake-on-produce arrives.
     *  Same-node production wakes a poller immediately; this bounds the latency for cross-node
     *  production (and any missed signal). Overridable via {@code WIGGLE_FALLBACK_POLL_MILLIS}. */
    private final long fallbackPollMillis = envLong("WIGGLE_FALLBACK_POLL_MILLIS", 100);

    /** Adaptive fallback ramp (opt-in): a freshly-parked poll re-claims quickly (fallback÷4, floor
     *  10ms) and doubles its wait on every empty unsignaled round up to the configured interval.
     *  Work produced on ANOTHER node shortly after this one parks — the common case under steady
     *  load, where a poller re-parks right before the next task lands — is discovered in the fast
     *  window instead of a uniform [0, fallback) delay. A notifier signal jumps the wait straight to
     *  the configured interval: a signal proves the LOCAL wake path is covering this node, so fast
     *  re-claims add DB load without adding discovery (an earlier reset-to-fast-on-signal kept polls
     *  in fast mode on busy nodes and measurably cost ceiling throughput). Extra cost is therefore
     *  ≤2 claims per park, independent of load. */
    private final boolean adaptiveFallbackPoll = Boolean.parseBoolean(
            System.getProperty("wiggle.adaptive.fallback",
                    System.getenv().getOrDefault("WIGGLE_ADAPTIVE_FALLBACK_POLL", "false")));

    /** Default doWhile iteration budget for loops that don't declare one: a loop guard may
     *  evaluate true at most this many times before the instance FAILS with a clear error. An
     *  unbounded loop with a buggy condition is a self-inflicted denial of service — it hot-spins
     *  workers and the database and grows the instance's token rows without limit — so every
     *  doWhile is budgeted; a loop that legitimately needs more says so in the topology. */
    final long loopMaxIterations = envLong("WIGGLE_LOOP_MAX_ITERATIONS", 10_000);

    /** Token-payload bookkeeping: per-loop-guard true-evaluation counts ({nodeId: n}), carried
     *  along the token chain and stripped from every dispatched context. */
    static final String LOOP_COUNTS = "__loops__";


    /** After a wake-on-produce signal, briefly let more tokens accumulate before claiming, so a burst
     *  is drained in one batched claim instead of a round trip per token. Trades up to this much
     *  first-token latency for fewer, larger claims under load; 0 disables (claim immediately). Only
     *  applies when the worker asked for more than one task (it has spare capacity to batch).
     *  Overridable via {@code WIGGLE_DISPATCH_LINGER_MILLIS}. */
    private final long dispatchLingerMillis = envLong("WIGGLE_DISPATCH_LINGER_MILLIS", 5);

    /** A non-negative long from {@code env}, or {@code def} if unset/blank/unparseable. */
    private static long envLong(String env, long def) {
        String v = System.getenv(env);
        if (v == null || v.isBlank()) return def;
        try {
            return Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            return def;
        }
    }

    private final Storage storage;
    private final Sagas sagas = new Sagas(this);
    private final Queries queries;
    private final DefinitionRegistry definitions;
    /** Who is polling this node and for what -- so the console can show unclaimable work. */
    private final PollerRegistry pollers = new PollerRegistry(60_000);
    private final long defaultLeaseMillis;
    private final java.util.function.Supplier<String> idMinter;

    /** Wake-on-produce for long-polling workers (Layer 1; see docs/in-memory-dispatch.md). */
    private final DispatchNotifier notifier = new DispatchNotifier();
    /** Queues that had a token parked READY during the in-flight transaction, flushed to the notifier
     *  after it commits. Set by {@link #parkAtWorkerStep}, drained by {@link #tx}/{@link #txVoid}. */
    private final ThreadLocal<Set<String>> readyQueues = new ThreadLocal<>();

    /** Marks {@code queue} for the post-commit wake-on-produce notification; null is a no-op. */
    void wakeQueue(String queue) {
        Set<String> ready = readyQueues.get();
        if (ready != null && queue != null) ready.add(queue);
    }

    /** Runs {@code body} in a transaction, then (post-commit) wakes pollers for any queue that had a
     *  token parked READY during it. Nesting is safe: an inner scope defers to the outermost. */
    private <T> T tx(java.util.function.Function<Tx, T> body) {
        Set<String> outer = readyQueues.get();
        Set<String> mine = new HashSet<>();
        readyQueues.set(mine);
        T result;
        try {
            result = storage.inTx(body);
        } finally {
            readyQueues.set(outer);
        }
        if (outer != null) outer.addAll(mine);   // let the outermost scope signal, post its commit
        else notifier.signal(mine);
        return result;
    }

    /** {@link #tx} for a body with no return value. */
    private void txVoid(java.util.function.Consumer<Tx> body) {
        Set<String> outer = readyQueues.get();
        Set<String> mine = new HashSet<>();
        readyQueues.set(mine);
        try {
            storage.inTxVoid(body);
        } finally {
            readyQueues.set(outer);
        }
        if (outer != null) outer.addAll(mine);
        else notifier.signal(mine);
    }

    public WorkflowEngine(Storage storage, DefinitionRegistry definitions, long defaultLeaseMillis) {
        this(storage, definitions, defaultLeaseMillis, () -> Ids.next("wfi"));
    }

    /** {@code idMinter} produces new instance ids: legacy {@code wfi_...} by default, or epoch-aware
     *  ids ({@link com.wiggle.core.IdCodec}) when the cell is placed under a coordinator. */
    public WorkflowEngine(Storage storage, DefinitionRegistry definitions, long defaultLeaseMillis,
                          java.util.function.Supplier<String> idMinter) {
        this.storage = storage;
        this.definitions = definitions;
        this.queries = new Queries(storage, pollers);
        this.defaultLeaseMillis = defaultLeaseMillis;
        this.idMinter = idMinter;
    }

    public DefinitionRegistry definitions() { return definitions; }

    // Facade over the registry so callers (API, dashboard) don't reach through the engine
    // into a collaborator's collaborator.

    /** Registers a definition (blob + normalised graph rows). */
    public WorkflowDefinition register(WorkflowDefinition def) { return definitions.register(def); }

    /** All registered workflow names. */
    public List<String> workflowNames() { return definitions.names(); }

    /** The most recently registered version of a workflow, if any. */
    public Optional<WorkflowDefinition> latestDefinition(String name) { return definitions.latest(name); }

    /**
     * The dispatchable backlog, split by (workflow, version, queue), each marked with whether any
     * worker polling this node would claim it. An uncovered slice is work nothing can pick up.
     */
    public List<Rows.BacklogSlice> backlog(int max) {
        return queries.backlog(max);
    }

    /** Whether a live poller on this node would claim this slice. See {@link PollerRegistry}. */
    public boolean covered(String workflow, int version, String queue) {
        return queries.covered(workflow, version, queue);
    }

    /** The workers currently polling this node. */
    public Set<PollerRegistry.Poller> livePollers() {
        return queries.livePollers();
    }

    /** One exact version -- what a version-scoped worker validates its handlers against. */
    public Optional<WorkflowDefinition> definition(String name, int version) {
        return definitions.lookup(name, version);
    }

    public String start(String workflow, Integer version, Object context, String correlationId) {
        return tx(tx -> startInTx(tx, workflow, version, context, correlationId, null));
    }

    /** Starts an instance inside an existing transaction; {@code parentTokenId} links a sub-workflow. */
    String startInTx(Tx tx, String workflow, Integer version, Object context,
                             String correlationId, String parentTokenId) {
        int v = version != null ? version : tx.latestVersion(workflow).orElseThrow(
                () -> EngineException.notFound("workflow '" + workflow + "'"));
        LazyGraph def = definitions.graph(tx, workflow, v);
        long now = System.currentTimeMillis();
        Instance inst = insertNewInstance(tx, def, context, correlationId, parentTokenId, now);
        Token t = newToken(inst, def.startNode(), "", null, now);
        LOG.log(System.Logger.Level.DEBUG, () -> "start: instance " + inst.id + " of " + def.key()
                + " at node " + def.startNode() + " correlationId=" + correlationId);
        drive(tx, def, inst, new ArrayDeque<>(List.of(t)), now);
        return inst.id;
    }

    private Instance insertNewInstance(Tx tx, LazyGraph def, Object context, String correlationId,
                                       String parentTokenId, long now) {
        Instance inst = new Instance();
        inst.id = idMinter.get();
        inst.workflow = def.name();
        inst.version = def.version();
        inst.correlationId = correlationId;
        inst.parentTokenId = parentTokenId;
        inst.status = InstanceStatus.RUNNING;
        inst.contextJson = Json.write(context == null ? Map.of() : context);
        inst.createdAt = now;
        inst.updatedAt = now;
        tx.insertInstance(inst);
        return inst;
    }

    public void cancel(String instanceId, String reason) {
        List<String> children = tx(tx -> {
            Instance inst = tx.lockInstance(instanceId).orElseThrow(() -> EngineException.notFound("instance"));
            if (inst.status != InstanceStatus.RUNNING) {
                LOG.log(System.Logger.Level.DEBUG, () ->
                        "cancel: instance " + instanceId + " ignored, already " + inst.status);
                return List.of();
            }
            long now = System.currentTimeMillis();
            cancelActiveTokens(tx, inst.id, now);
            inst.status = InstanceStatus.CANCELLED;
            inst.terminationReason = reason;
            inst.updatedAt = now;
            tx.updateInstance(inst);
            notifyParent(tx, inst, now);
            LOG.log(System.Logger.Level.DEBUG, () -> "cancel: instance " + instanceId + " cancelled, reason=" + reason);
            return tx.childInstanceIds(instanceId);
        });
        // Cancelling in separate transactions keeps lock ordering one-way (child -> parent only).
        for (String child : children) {
            try {
                cancel(child, "parent instance cancelled");
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.WARNING, "cascade cancel of " + child + " failed: " + e);
            }
        }
    }

    public Optional<InstanceView> instance(String id) {
        return queries.instance(id);
    }

    public List<InstanceView> list(String workflow, String status, int limit) {
        return queries.list(workflow, status, limit);
    }

    /** Instances started with {@code correlationId} (a business key), newest first. */
    public List<InstanceView> findByCorrelation(String correlationId, int limit) {
        return queries.findByCorrelation(correlationId, limit);
    }

    public List<Token> tokens(String instanceId) {
        return queries.tokens(instanceId);
    }

    /**
     * Live (RUNNING) instance count grouped by the epoch encoded in each instance id -- this cell's
     * contribution to the coordinator's retire census (R21). A DRAINING epoch that reaches zero here on
     * every cell can be retired. Legacy ids (no epoch) count as the genesis epoch 0.
     */
    public Map<Long, Integer> liveCountByEpoch() {
        return queries.liveCountByEpoch();
    }

    /** Snapshot of the dispatchable backlog right now: how many tasks are queued and waiting. */
    public Rows.QueueDepth queueDepth() {
        return queries.queueDepth();
    }

    /**
     * Worker-dispatched tasks completed since {@code since}, across every node in the cluster --
     * the consumption-rate signal for lag monitoring.
     */
    public int tasksProcessedSince(long since) {
        return queries.tasksProcessedSince(since);
    }

    /** Leases up to {@code max} tasks for a worker. Returns immediately; long-polling lives in the HTTP layer. */
    public List<TaskActivation> poll(String workerId, Set<String> queues, int max, Long leaseMillis) {
        return poll(workerId, queues, max, leaseMillis, 0);
    }

    public List<TaskActivation> poll(String workerId, Set<String> queues, int max, Long leaseMillis, long deadline) {
        return poll(workerId, queues, max, leaseMillis, deadline, () -> false);
    }

    public List<TaskActivation> poll(String workerId, Set<String> queues, int max, Long leaseMillis, long deadline,
                                     java.util.function.BooleanSupplier cancelled) {
        return poll(workerId, queues, null, max, leaseMillis, deadline, cancelled);
    }

    /**
     * Long-polls for work. {@code cancelled} lets the caller (the gRPC layer) signal that the worker's
     * request is gone -- a closing or dead worker whose call was cancelled -- so we do not claim a
     * token for a worker that will never run it (which would only strand it until lease expiry). This
     * matters with wake-on-produce: a signal can wake a parked poll the instant its worker is shutting
     * down, and the freshly-produced token should go to a live worker instead.
     */
    public List<TaskActivation> poll(String workerId, Set<String> queues,
                                     Set<com.wiggle.core.WorkflowVersion> versions, int max,
                                     Long leaseMillis, long deadline,
                                     java.util.function.BooleanSupplier cancelled) {
        pollers.seen(workerId, queues, versions, System.currentTimeMillis());
        long lease = leaseMillis == null || leaseMillis <= 0 ? defaultLeaseMillis : leaseMillis;
        if (cancelled.getAsBoolean()) return List.of();
        Map<String, Long> since = notifier.snapshot(queues);
        List<TaskActivation> tasks = claimNow(workerId, queues, versions, max, lease);
        long rampStart = Math.max(10, fallbackPollMillis / 4);
        long fallbackWait = adaptiveFallbackPoll ? rampStart : fallbackPollMillis;
        while (tasks.isEmpty() && System.currentTimeMillis() < deadline) {
            long remaining = deadline - System.currentTimeMillis();
            boolean signaled = notifier.awaitChange(queues, since, Math.min(fallbackWait, remaining));
            if (signaled && max > 1) lingerForBatch(deadline);
            if (cancelled.getAsBoolean()) return List.of();   // worker gone -- leave the work for a live one
            since = notifier.snapshot(queues);
            tasks = claimNow(workerId, queues, versions, max, lease);
            if (adaptiveFallbackPoll) {
                fallbackWait = signaled ? fallbackPollMillis : Math.min(fallbackWait * 2, fallbackPollMillis);
            }
        }
        if (!tasks.isEmpty()) {
            List<TaskActivation> claimed = tasks;
            LOG.log(System.Logger.Level.DEBUG, () -> "poll: worker " + workerId + " queues=" + queues
                    + " claimed " + claimed.size() + " task(s): "
                    + claimed.stream().map(a -> a.taskId() + "@" + a.stepName()).toList());
        }
        return tasks;
    }

    /** One atomic DB claim attempt, with a lease that starts now (not at the poll's arrival). */
    private List<TaskActivation> claimNow(String workerId, Set<String> queues,
                                          Set<com.wiggle.core.WorkflowVersion> versions,
                                          int max, long lease) {
        long now = System.currentTimeMillis();
        return storage.inTx(tx -> claimActivations(tx, workerId, queues, versions, max, now, now + lease));
    }

    /** Coalesce a burst: wait up to {@link #dispatchLingerMillis} (bounded by the poll deadline) so
     *  concurrently-produced tokens are claimed together instead of one per round trip. */
    private void lingerForBatch(long deadline) {
        if (dispatchLingerMillis <= 0) return;
        long budget = Math.min(dispatchLingerMillis, deadline - System.currentTimeMillis());
        if (budget <= 0) return;
        try {
            Thread.sleep(budget);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private List<TaskActivation> claimActivations(Tx tx, String workerId, Set<String> queues,
                                                  Set<com.wiggle.core.WorkflowVersion> versions,
                                                  int max, long now, long until) {
        List<Token> claimed = tx.claimTasks(workerId, queues, versions, max, now, until);
        Map<String, Instance> instances = instancesOf(tx, claimed);
        List<TaskActivation> activations = new ArrayList<>(claimed.size());
        for (Token t : claimed) {
            activationFor(tx, instances.get(t.instanceId), t, workerId, until).ifPresent(activations::add);
        }
        return activations;
    }

    /** The claimed batch's instances in one read; an instance can be gone (purged) -> absent. */
    private static Map<String, Instance> instancesOf(Tx tx, List<Token> claimed) {
        if (claimed.isEmpty()) return Map.of();
        Set<String> ids = new LinkedHashSet<>();
        for (Token t : claimed) ids.add(t.instanceId);
        Map<String, Instance> out = new HashMap<>();
        for (Instance i : tx.findInstances(ids)) out.put(i.id, i);
        return out;
    }

    private Optional<TaskActivation> activationFor(Tx tx, Instance inst, Token t, String workerId, long until) {
        boolean comp = Sagas.isCompensation(t);
        if (inst == null || inst.status != (comp ? InstanceStatus.COMPENSATING : InstanceStatus.RUNNING)) {
            return Optional.empty();
        }
        if (comp) return Optional.of(Sagas.activation(inst, t, workerId, until));
        Node node = definitions.graph(tx, t.workflow, t.version).node(t.nodeId);
        ExecutionMode mode = resolveMode(definitions.executionMode(tx, t.workflow, t.version));
        Object base = null;
        long itemIndex = 0;
        String itemMapKey = null;
        if (isItemToken(t)) {
            Map<String, Object> payload = Json.parseObject(t.payloadJson);
            base = itemBaseContext(inst, t);
            itemIndex = ((Number) payload.get(ARM_IDX)).longValue();
            itemMapKey = payload.get(ITEM_MAP_KEY) == null ? null : String.valueOf(payload.get(ITEM_MAP_KEY));
        }
        return Optional.of(new TaskActivation(t.id, inst.id, inst.workflow, inst.version, node.id(), node.name(),
                node.activity(), node.kind(), t.attempt + 1, until, workerId, dispatchContext(inst, t),
                base, itemIndex, itemMapKey, mode));
    }

    /** Extends the lease of an in-flight task (worker heartbeat for long-running steps). */
    public long extendLease(String taskId, String leaseOwner, long extraMillis) {
        long until = storage.inTx(tx -> {
            Token t = tx.findToken(taskId).orElseThrow(() -> EngineException.notFound("task"));
            requireLease(t, leaseOwner);
            t.leaseExpiresAt = System.currentTimeMillis() + extraMillis;
            t.updatedAt = System.currentTimeMillis();
            tx.updateToken(t);
            return t.leaseExpiresAt;
        });
        LOG.log(System.Logger.Level.DEBUG, () ->
                "extendLease: task " + taskId + " owner=" + leaseOwner + " now expires at " + until);
        return until;
    }

    /** A task's token re-read under its instance's write lock. */
    private record LockedTask(Instance inst, Token token) {}

    /** Takes the instance's write lock (resolved from the task in the same statement), then reads
     *  the token under it. */
    private static LockedTask lockTask(Tx tx, String taskId) {
        Instance inst = tx.lockInstanceOfTask(taskId).orElseThrow(() -> EngineException.notFound("task"));
        Token token = tx.findToken(taskId).orElseThrow(() -> EngineException.notFound("task"));
        return new LockedTask(inst, token);
    }

    private static void requireRunning(Instance inst) {
        if (inst.status != InstanceStatus.RUNNING) {
            throw EngineException.conflict("instance " + inst.id + " is " + inst.status);
        }
    }

    /**
     * Completes a task. For TASK nodes (combines included) {@code result} REPLACES the context —
     * the handler's return is the complete next context, sent whole by the worker; a null result
     * leaves the context untouched. For PREDICATE nodes it must carry a boolean under
     * {@code "value"}.
     */
    public void complete(String taskId, String leaseOwner, Object result) {
        txVoid(tx -> {
            LockedTask locked = lockTask(tx, taskId);
            Instance inst = locked.inst();
            Token t = locked.token();
            requireLease(t, leaseOwner);
            long now = System.currentTimeMillis();
            Long compSeq = Sagas.seqOf(t);
            if (compSeq != null) {                          // the reverse pass: a compensator finished
                sagas.complete(tx, inst, t, compSeq, now);
                return;
            }
            requireRunning(inst);
            LazyGraph def = definitions.graph(tx, t.workflow, t.version);
            Node node = def.node(t.nodeId);
            NodeBehaviours behaviour = NodeBehaviours.of(node.kind());
            Object compInput = node.compensable() ? dispatchContext(inst, t) : null;
            String next = behaviour.route(inst, t, node, result);
            if (node.compensable()) Sagas.capture(tx, inst, t, node, compInput, now);
            String overrun = behaviour.overrunAfter(this, t, node, result);
            if (overrun != null) {
                settleToken(tx, t, now);
                failInstance(tx, inst, overrun, now);
                return;
            }
            settleToken(tx, t, now);
            touchInstance(tx, inst, now);
            Token cont = newToken(inst, next, t.joinStack, stripCombineScratch(node, t.payloadJson), now);
            drive(tx, def, inst, new ArrayDeque<>(List.of(cont)), now);
        });
    }

    /** First write inserts, later writes update: a fresh token reaches the store once, already in
     *  its parked state. */
    static void saveToken(Tx tx, Token t) {
        if (t.persisted) {
            tx.updateToken(t);
        } else {
            tx.insertToken(t);
            t.persisted = true;
        }
    }

    /** Marks a token consumed and releases its lease. */
    static void settleToken(Tx tx, Token t, long now) {
        t.status = TokenStatus.DONE;
        t.leaseOwner = null;
        t.leaseExpiresAt = 0;
        t.updatedAt = now;
        tx.updateToken(t);
    }

    static void touchInstance(Tx tx, Instance inst, long now) {
        inst.updatedAt = now;
        tx.updateInstance(inst);
    }

    /** DEFAULT resolves to the reference {@link ExecutionMode#SERVER} for now (no server-wide override yet). */
    private static ExecutionMode resolveMode(ExecutionMode mode) {
        return mode == null || mode == ExecutionMode.DEFAULT ? ExecutionMode.SERVER : mode;
    }

    /** One locally-executed step reported by a worker: a task merge, or a predicate value. */
    public record StepInput(String nodeId, Object merge, Boolean predicateValue) {}

    /** The result of applying a reported run: the instance's status, renewed lease, and next token. */
    public record AdvanceOutcome(String instanceStatus, long leaseExpiresAt, String nextTaskId) {}

    /**
     * Applies an ordered run of locally-executed steps (LOCAL_SYNC/LOCAL_ASYNC) atomically under
     * the instance lock. For each step it does exactly what {@link #complete} would: merge the
     * task result or route the predicate, advance the token. Between steps the continuation is
     * leased straight back to the same worker (never exposed to {@code poll}); at the final step
     * (or a boundary) it is driven normally, releasing the worker.
     */
    public AdvanceOutcome advance(String startTaskId, String leaseOwner, List<StepInput> steps, boolean finalHandback) {
        return tx(tx -> {
            LockedTask locked = lockTask(tx, startTaskId);
            Instance inst = locked.inst();
            long now = System.currentTimeMillis();
            long lease = now + defaultLeaseMillis;
            if (inst.status != InstanceStatus.RUNNING) {
                return new AdvanceOutcome(inst.status.name(), 0, null);
            }
            requireLease(locked.token(), leaseOwner);
            LazyGraph def = definitions.graph(tx, inst.workflow, inst.version);
            return applyRun(tx, def, inst, locked.token(), leaseOwner, steps, finalHandback, now, lease);
        });
    }

    private AdvanceOutcome applyRun(Tx tx, LazyGraph def, Instance inst, Token current, String leaseOwner,
                                    List<StepInput> steps, boolean finalHandback, long now, long lease) {
        String nextTaskId = null;
        for (int i = 0; i < steps.size(); i++) {
            StepInput step = steps.get(i);
            Node node = def.node(current.nodeId);
            requireReportedNode(node, step, current);
            NodeBehaviours behaviour = NodeBehaviours.of(node.kind());
            Object compInput = node.compensable() ? dispatchContext(inst, current) : null;
            String next = behaviour.routeReported(inst, current, node, step);
            if (node.compensable()) Sagas.capture(tx, inst, current, node, compInput, now);
            String overrun = behaviour.overrunReported(this, current, node, step);
            if (overrun != null) {
                settleToken(tx, current, now);
                failInstance(tx, inst, overrun, now);
                return new AdvanceOutcome(inst.status.name(), 0, null);
            }
            settleToken(tx, current, now);
            touchInstance(tx, inst, now);
            Token cont = newToken(inst, next, current.joinStack, stripCombineScratch(node, current.payloadJson), now);
            Node nextNode = def.node(next);
            boolean lastStep = i == steps.size() - 1;
            if ((lastStep && finalHandback) || !nextNode.isWorkerDispatched()) {
                handBack(tx, def, inst, cont, nextNode, now);
                return new AdvanceOutcome(inst.status.name(), lease, null);
            }
            leaseBack(tx, cont, nextNode, leaseOwner, lease, now);
            LOG.log(System.Logger.Level.DEBUG, () -> "advanceRun: instance " + inst.id
                    + " chaining locally " + node.name() + " -> " + next);
            current = cont;
            nextTaskId = cont.id;
        }
        return new AdvanceOutcome(inst.status.name(), lease, nextTaskId);
    }

    private static void requireReportedNode(Node node, StepInput step, Token current) {
        if (!node.id().equals(step.nodeId())) {
            throw EngineException.conflict("reported step " + step.nodeId() + " but token "
                    + current.id + " is at " + node.id());
        }
    }

    /** Hand back: drive the continuation normally (READY for a worker, or a boundary). */
    private void handBack(Tx tx, LazyGraph def, Instance inst, Token cont, Node nextNode, long now) {
        LOG.log(System.Logger.Level.DEBUG, () -> "advanceRun: instance " + inst.id
                + " handing back at " + cont.nodeId + " (" + nextNode.kind() + ")");
        drive(tx, def, inst, new ArrayDeque<>(List.of(cont)), now);
    }

    /** Keep the chain on this worker: lease the continuation straight back, no poll. */
    private static void leaseBack(Tx tx, Token cont, Node nextNode, String leaseOwner, long lease, long now) {
        cont.status = TokenStatus.RUNNING;
        cont.kind = nextNode.kind();
        cont.activity = nextNode.activity();
        cont.queue = nextNode.queue();
        cont.leaseOwner = leaseOwner;
        cont.leaseExpiresAt = lease;
        cont.availableAt = now;
        cont.updatedAt = now;
        saveToken(tx, cont);
    }


    /** The signal waits currently pending an external delivery, oldest first. */
    public List<Token> pendingSignals(int max) {
        return queries.pendingSignals(max);
    }

    /**
     * Delivers a named signal to an instance. The instance must currently be waiting on that
     * signal (there is no buffering; an early signal is a conflict the sender can retry).
     * {@code payload} merges into the context and the flow advances down the signal's path.
     */
    public void signal(String instanceId, String name, Object payload) {
        txVoid(tx -> {
            Instance inst = tx.lockInstance(instanceId).orElseThrow(() -> EngineException.notFound("instance"));
            requireRunning(inst);
            Token t = tx.tokensOf(instanceId).stream()
                    .filter(x -> x.status == TokenStatus.AWAITING && x.kind == NodeKind.SIGNAL)
                    .filter(x -> name.equals(x.activity))
                    .findFirst()
                    .orElseThrow(() -> EngineException.conflict(
                            "instance " + instanceId + " is not waiting for signal '" + name + "'"));
            long now = System.currentTimeMillis();
            LazyGraph def = definitions.graph(tx, t.workflow, t.version);
            Node node = def.node(t.nodeId);
            mergeContext(inst, payload);
            settleToken(tx, t, now);
            touchInstance(tx, inst, now);
            Token cont = newToken(inst, node.next(), t.joinStack, t.payloadJson, now);
            LOG.log(System.Logger.Level.DEBUG, () -> "signal: '" + name + "' delivered to instance "
                    + inst.id + " -> " + node.next());
            drive(tx, def, inst, new ArrayDeque<>(List.of(cont)), now);
        });
    }

    /** A per-token action executed in its own transaction by a leader sweep. */
    private interface SweepAction {
        void apply(Tx tx, Token token);
    }

    /** Runs {@code action} once per token, each in its own transaction, isolating failures. */
    private int sweep(List<Token> due, String what, SweepAction action) {
        int done = 0;
        for (Token token : due) {
            try {
                txVoid(tx -> action.apply(tx, token));
                done++;
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.WARNING, what + " " + token.id + " failed: " + e);
            }
        }
        return done;
    }

    private static void logDue(String what, List<Token> due) {
        if (!due.isEmpty()) {
            LOG.log(System.Logger.Level.DEBUG, () -> what + ": " + due.size() + " due");
        }
    }

    /** Leader duty: advance sleep timers that have come due. */
    public int fireDueTimers(int max) {
        List<Token> due = storage.inTx(tx -> tx.dueTimers(System.currentTimeMillis(), max));
        logDue("fireDueTimers", due);
        return sweep(due, "timer", this::fireTimer);
    }

    private void fireTimer(Tx tx, Token timer) {
        Instance inst = tx.lockInstance(timer.instanceId).orElse(null);
        if (inst == null || inst.status != InstanceStatus.RUNNING) return;
        Token t = tx.findToken(timer.id).orElse(null);
        if (t == null || t.status != TokenStatus.WAITING) return;
        long ts = System.currentTimeMillis();
        LazyGraph def = definitions.graph(tx, t.workflow, t.version);
        Node node = def.node(t.nodeId);
        t.status = TokenStatus.DONE;
        t.updatedAt = ts;
        tx.updateToken(t);
        Token cont = newToken(inst, node.next(), t.joinStack, t.payloadJson, ts);
        LOG.log(System.Logger.Level.DEBUG, () -> "timer " + node.name()
                + " of instance " + inst.id + " fired -> " + node.next());
        drive(tx, def, inst, new ArrayDeque<>(List.of(cont)), ts);
    }

    /** Leader duty: signal waits whose deadline has passed escalate (to {@code altNext}) or fail. */
    public int fireDueSignalDeadlines(int max) {
        List<Token> due = storage.inTx(tx -> tx.dueSignals(System.currentTimeMillis(), max));
        logDue("fireDueSignalDeadlines", due);
        return sweep(due, "signal deadline", this::escalateOrFailSignal);
    }

    private void escalateOrFailSignal(Tx tx, Token task) {
        Instance inst = tx.lockInstance(task.instanceId).orElse(null);
        if (inst == null || inst.status != InstanceStatus.RUNNING) return;
        Token t = tx.findToken(task.id).orElse(null);
        if (t == null || t.status != TokenStatus.AWAITING) return;
        long ts = System.currentTimeMillis();
        if (t.availableAt <= 0 || t.availableAt > ts) return;   // deadline cleared or moved
        LazyGraph def = definitions.graph(tx, t.workflow, t.version);
        Node node = def.node(t.nodeId);
        t.status = TokenStatus.DONE;
        t.updatedAt = ts;
        tx.updateToken(t);
        if (node.altNext() == null) {
            LOG.log(System.Logger.Level.DEBUG, () -> "signal " + node.name()
                    + " of instance " + inst.id + " missed its deadline, no escalation -> failing instance");
            failInstance(tx, inst, "signal '" + node.name() + "' timed out", ts);
            return;
        }
        Token cont = newToken(inst, node.altNext(), t.joinStack, t.payloadJson, ts);
        LOG.log(System.Logger.Level.DEBUG, () -> "signal " + node.name()
                + " of instance " + inst.id + " missed its deadline -> escalating to " + node.altNext());
        drive(tx, def, inst, new ArrayDeque<>(List.of(cont)), ts);
    }

    /** Leader duty: return tasks whose worker died back to the ready pool. */
    public int reclaimExpiredLeases(int max) {
        List<Token> orphans = storage.inTx(tx -> tx.expiredLeases(System.currentTimeMillis(), max));
        logDue("reclaimExpiredLeases", orphans);
        return sweep(orphans, "reclaim of", this::reclaimOrphan);
    }

    private void reclaimOrphan(Tx tx, Token orphan) {
        Instance inst = tx.lockInstance(orphan.instanceId).orElse(null);
        if (inst == null) return;
        Token t = tx.findToken(orphan.id).orElse(null);
        if (t == null || t.status != TokenStatus.RUNNING || t.leaseExpiresAt >= System.currentTimeMillis()) return;
        Node node = definitions.graph(tx, t.workflow, t.version).node(t.nodeId);
        long ts = System.currentTimeMillis();
        LOG.log(System.Logger.Level.DEBUG, () -> "reclaim: " + node.name() + " of instance " + inst.id
                + " orphaned by worker " + t.leaseOwner);
        failToken(tx, inst, t, node, "lease expired (worker unreachable)", "lease expired", true, ts);
    }

    /** Fails a task. Retries per the node's policy; when exhausted the whole instance fails. */
    public void fail(String taskId, String leaseOwner, String message, boolean retryable) {
        txVoid(tx -> {
            LockedTask locked = lockTask(tx, taskId);
            Instance inst = locked.inst();
            Token t = locked.token();
            requireLease(t, leaseOwner);
            // COMPENSATING instances still have live work in flight — their compensators. A
            // compensator's failure report must reach failToken (-> COMPENSATION_FAILED), not
            // be dropped by the terminal-status guard.
            if (inst.status != InstanceStatus.RUNNING && inst.status != InstanceStatus.COMPENSATING) return;
            Node node = definitions.graph(tx, t.workflow, t.version).node(t.nodeId);
            failToken(tx, inst, t, node, message, message, retryable, System.currentTimeMillis());
        });
    }

    /**
     * The shared retry-or-fail transition: bumps the attempt, releases the lease, then either
     * reschedules the token per the node's retry policy or fails it (and, if the instance is
     * still running, the whole instance -- as {@code node.name() + ": " + failReason}).
     */
    private void failToken(Tx tx, Instance inst, Token t, Node node,
                           String lastError, String failReason, boolean retryable, long now) {
        RetryPolicy policy = node.retry() == null ? RetryPolicy.forever() : node.retry();
        t.attempt++;
        t.lastError = lastError;
        t.leaseOwner = null;
        t.leaseExpiresAt = 0;
        t.updatedAt = now;
        if (retryable && t.attempt < policy.maxAttempts()) {
            t.status = TokenStatus.READY;
            t.availableAt = now + policy.backoffMillis(t.attempt);
            tx.updateToken(t);
            long backoffMs = t.availableAt - now;
            LOG.log(System.Logger.Level.DEBUG, () -> "fail: " + node.name() + " of instance " + inst.id
                    + " failed (" + lastError + "), retrying attempt " + t.attempt
                    + "/" + policy.maxAttempts() + " in " + backoffMs + "ms");
            return;
        }
        t.status = TokenStatus.FAILED;
        tx.updateToken(t);
        LOG.log(System.Logger.Level.DEBUG, () -> "fail: " + node.name() + " of instance " + inst.id
                + " exhausted retries (attempt " + t.attempt + "/" + policy.maxAttempts()
                + ", retryable=" + retryable + ") -> failing instance");
        Long compSeq = Sagas.seqOf(t);
        if (compSeq != null) {
            sagas.compensatorExhausted(tx, inst, node, compSeq, failReason, now);
            return;
        }
        if (inst.status == InstanceStatus.RUNNING) {
            failInstance(tx, inst, node.name() + ": " + failReason, now);
        }
    }

    /** Creates a recurring start: {@code workflow} fires every {@code every}, first fire after one interval. */
    public String createSchedule(String workflow, java.time.Duration every, Object context) {
        if (every.toMillis() < 1) throw EngineException.badRequest("schedule interval must be positive");
        Rows.Schedule s = new Rows.Schedule();
        s.intervalMillis = every.toMillis();
        s.nextFireAt = System.currentTimeMillis() + s.intervalMillis;
        return putSchedule(workflow, context, s, "every " + s.intervalMillis + "ms");
    }

    /** Creates a recurring start on a five-field cron expression (evaluated in UTC). */
    public String createCronSchedule(String workflow, String cron, Object context) {
        com.wiggle.core.Cron parsed;
        try {
            parsed = com.wiggle.core.Cron.parse(cron);
        } catch (IllegalArgumentException e) {
            throw EngineException.badRequest(e.getMessage());
        }
        Rows.Schedule s = new Rows.Schedule();
        s.cron = parsed.expression();
        s.nextFireAt = parsed.next(System.currentTimeMillis());
        return putSchedule(workflow, context, s, "cron '" + s.cron + "'");
    }

    /**
     * Upserts by workflow: a workflow has at most one schedule, so re-creating one (e.g. from
     * several app instances doing "ensure my schedule exists" on startup) updates the existing
     * row's cadence/context in place instead of piling up duplicate firers.
     */
    private String putSchedule(String workflow, Object context, Rows.Schedule s, String cadence) {
        return storage.inTx(tx -> {
            tx.latestVersion(workflow).orElseThrow(
                    () -> EngineException.notFound("workflow '" + workflow + "'"));
            java.util.Optional<Rows.Schedule> existing = tx.scheduleByWorkflow(workflow);
            s.id = existing.map(e -> e.id).orElseGet(() -> Ids.next("sched"));
            s.workflow = workflow;
            s.contextJson = Json.write(context == null ? Map.of() : context);
            s.createdAt = existing.map(e -> e.createdAt).orElseGet(System::currentTimeMillis);
            tx.putSchedule(s);
            boolean replaced = existing.isPresent();
            LOG.log(System.Logger.Level.INFO, () -> "schedule " + s.id + ": " + workflow + " " + cadence
                    + (replaced ? " (replacing existing schedule for this workflow)" : ""));
            return s.id;
        });
    }

    public void deleteSchedule(String id) {
        storage.inTxVoid(tx -> tx.deleteSchedule(id));
    }

    public List<Rows.Schedule> schedules() {
        return storage.inTx(Tx::schedules);
    }

    /**
     * Leader duty: start instances for schedules whose fire time has passed. The compare-and-set
     * on the fire time makes each fire exactly-once even if two leaders briefly overlap; missed
     * fires do not burst -- the next fire is one interval from now.
     */
    public int fireDueSchedules(int max) {
        long now = System.currentTimeMillis();
        List<Rows.Schedule> due = storage.inTx(tx -> tx.dueSchedules(now, max));
        int fired = 0;
        for (Rows.Schedule sched : due) {
            try {
                if (fireSchedule(sched, now)) fired++;
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.WARNING, "schedule " + sched.id + " failed to fire: " + e);
            }
        }
        return fired;
    }

    private boolean fireSchedule(Rows.Schedule sched, long now) {
        return tx(tx -> {
            if (!tx.claimSchedule(sched.id, sched.nextFireAt, nextFire(sched, now))) return false;
            String id = startInTx(tx, sched.workflow, null, Json.parse(sched.contextJson),
                    "schedule:" + sched.id, null);
            LOG.log(System.Logger.Level.DEBUG, () -> "schedule " + sched.id + " fired -> instance " + id);
            return true;
        });
    }

    /** Next fire after {@code now}: one interval ahead, or the cron's next UTC match. */
    private static long nextFire(Rows.Schedule sched, long now) {
        if (sched.cron == null) return now + sched.intervalMillis;
        return com.wiggle.core.Cron.parse(sched.cron).next(now);
    }

    public int purgeTerminalInstancesOlderThan(long retentionMillis, int max) {
        long cutoff = System.currentTimeMillis() - retentionMillis;
        int purged = storage.inTx(tx -> tx.deleteTerminalInstancesBefore(cutoff, max));
        if (purged > 0) {
            LOG.log(System.Logger.Level.DEBUG, () -> "purgeTerminalInstancesOlderThan: removed " + purged
                    + " instance(s) updated before " + cutoff);
        }
        return purged;
    }

    private static void requireLease(Token t, String leaseOwner) {
        if (t.status != TokenStatus.RUNNING) {
            throw EngineException.conflict("task " + t.id + " is " + t.status + ", not RUNNING");
        }
        if (leaseOwner != null && !leaseOwner.equals(t.leaseOwner)) {
            throw EngineException.conflict("lease for task " + t.id + " is held by " + t.leaseOwner);
        }
    }

    /**
     * Reserved payload key tagging a token with its 0-based arm index inside the enclosing static
     * fork. Its presence marks a token as running in an <em>isolated branch</em>: the branch's step
     * results merge into its own payload overlay ({@link #applyStepResult}) instead of the shared
     * context, so siblings never see each other's writes and there is no implicit merge. It is
     * stripped before dispatch ({@link #dispatchContext}) and consumed at the join.
     */
    static final String ARM_IDX = "__armIdx__";

    /** True when {@code t} runs inside an isolated fork branch (its payload carries the arm tag). */
    private static boolean inScopedBranch(Token t) {
        return t.payloadJson != null && Json.parseObject(t.payloadJson).containsKey(ARM_IDX);
    }

    /** Internal bookkeeping on a forEach item token: the map key its element came from (map input
     *  only). Never reaches user context, like {@link #ARM_IDX}. */
    static final String ITEM_MAP_KEY = "__itemMapKey__";

    /** A forEach item token's working value: the element itself (any JSON value, scalars included).
     *  The item's branch context IS this value — item steps receive it as their context, their
     *  return replaces it, and the join collects the final values for the combine. */
    static final String ITEM_VALUE = "__item__";

    /** True when {@code t} is a forEach item token (its payload carries the item value slot). */
    private static boolean isItemToken(Token t) {
        return t.payloadJson != null && Json.parseObject(t.payloadJson).containsKey(ITEM_VALUE);
    }

    /** A fork combine's itemsKey is a JSON ARRAY of arm names; a forEach combine's is a JSON STRING
     *  naming the scratch key its collected results are staged under. */
    static String forEachScratchKey(Node combineNode) {
        Object parsed = Json.parse(combineNode.itemsKey());
        return parsed instanceof String s ? s : null;
    }

    /** A combine aggregator carries its arm names (a JSON array) on the node's itemsKey -- a field
     *  that round-trips through every store, unlike a TASK node's edge-derived branches (see
     *  {@code Pipeline.addAggregator}). */
    static boolean isCombineNode(Node node) {
        return node != null && node.kind() == NodeKind.TASK && node.itemsKey() != null;
    }

    /** The branch (arm) names a combine node keys its inputs by, in fork order. */
    static List<String> armNames(Node combineNode) {
        return Json.asArray(Json.parse(combineNode.itemsKey())).stream().map(String::valueOf).toList();
    }

    /** Once a combine node has run, its scratch keys have served their purpose: drop them (arm
     *  names for a fork, the collected-results key for a forEach) from the continuation payload so
     *  they never leak downstream. A no-op for any other node. */
    private static String stripCombineScratch(Node node, String payloadJson) {
        if (!isCombineNode(node) || payloadJson == null) return payloadJson;
        Map<String, Object> overlay = Json.parseObject(payloadJson);
        String scratch = forEachScratchKey(node);
        boolean changed = scratch != null
                ? overlay.remove(scratch) != null
                : overlay.keySet().removeAll(armNames(node).stream().map(ScratchKeys::arm).toList());
        return changed ? Json.write(overlay) : payloadJson;
    }

    /**
     * Applies a step result where it belongs: a branch's private overlay when scoped, else shared.
     * In every case the handler's return REPLACES the previous value — it is the complete next
     * context, and keys it omits do not survive. Step execution never diffs or merges; the only
     * merges are signal payloads and a sub-workflow's result folding into its parent. A null return
     * leaves the context untouched.
     */
    static void applyStepResult(Instance inst, Token t, Node node, Object result) {
        if (isCombineNode(node)) { replaceCombineResult(inst, t, node, result); return; }
        if (result == null) return;
        if (isItemToken(t)) {
            // A forEach item step: the return replaces the ITEM's working value (base untouched).
            Map<String, Object> payload = Json.parseObject(t.payloadJson);
            payload.put(ITEM_VALUE, dropNulls(result));
            t.payloadJson = Json.write(payload);
        } else if (inScopedBranch(t)) {
            t.payloadJson = overlayReplace(t.payloadJson, result);
        } else {
            inst.contextJson = Json.write(dropNulls(result));
        }
    }

    /** A top-level null value means "this key is absent" — never persist literal JSON nulls. */
    private static Object dropNulls(Object result) {
        if (!(result instanceof Map<?, ?> m)) return result;
        Map<String, Object> out = new LinkedHashMap<>();
        m.forEach((k, v) -> { if (v != null) out.put(String.valueOf(k), v); });
        return out;
    }

    /**
     * A combine's return IS the complete post-join context: it replaces (never merges into) the
     * shared context — or, when the join sits inside an enclosing fork branch, that branch's
     * overlay. The return is taken verbatim (an arm NAME may legitimately double as a data key, so
     * nothing user-visible is stripped); only the internal arm tag is removed defensively. A null
     * return leaves the context untouched.
     */
    private static void replaceCombineResult(Instance inst, Token t, Node node, Object result) {
        if (result == null) return;
        Object cleaned = result;
        if (result instanceof Map<?, ?> m) {
            Map<String, Object> out = new LinkedHashMap<>();
            m.forEach((k, v) -> out.put(String.valueOf(k), v));
            out.remove(ARM_IDX);
            out.remove(LOOP_COUNTS);
            cleaned = out;
        }
        if (inScopedBranch(t)) {
            // A nested join inside an outer branch: the return becomes that branch's overlay (the
            // outer combine still decides what ultimately lands in the shared context).
            Object arm = Json.parseObject(t.payloadJson).get(ARM_IDX);
            Map<String, Object> overlay = cleaned instanceof Map
                    ? Json.asObject(cleaned) : Json.parseObject(Json.write(cleaned));
            overlay.put(ARM_IDX, arm);
            t.payloadJson = Json.write(overlay);
        } else {
            inst.contextJson = Json.write(cleaned);
        }
    }

    /** Replaces a branch's private overlay with the step's return (the branch's complete new view),
     *  preserving only the internal bookkeeping keys (arm tag; a forEach item's source map key). */
    private static String overlayReplace(String payloadJson, Object result) {
        Map<String, Object> prev = payloadJson == null ? Map.of() : Json.parseObject(payloadJson);
        Map<String, Object> overlay = result instanceof Map
                ? Json.asObject(dropNulls(result)) : Json.parseObject(Json.write(result));
        if (prev.containsKey(ARM_IDX)) overlay.put(ARM_IDX, prev.get(ARM_IDX));
        if (prev.containsKey(ITEM_MAP_KEY)) overlay.put(ITEM_MAP_KEY, prev.get(ITEM_MAP_KEY));
        if (prev.containsKey(LOOP_COUNTS)) overlay.put(LOOP_COUNTS, prev.get(LOOP_COUNTS));
        return Json.write(overlay);
    }

    /** The one remaining merge: EXTERNAL inputs folding into the context — a delivered signal's
     *  payload and a completed sub-workflow's result. Step returns never come through here; they
     *  replace ({@link #applyStepResult}). */
    private static void mergeContext(Instance inst, Object result) {
        if (result == null) return;
        if (!(result instanceof Map)) {
            inst.contextJson = Json.write(result);
            return;
        }
        Map<String, Object> ctx = Json.parseObject(inst.contextJson);
        // A null value deletes its key; any other value overwrites.
        for (Map.Entry<?, ?> e : ((Map<?, ?>) result).entrySet()) {
            String key = String.valueOf(e.getKey());
            if (e.getValue() == null) ctx.remove(key);
            else ctx.put(key, e.getValue());
        }
        inst.contextJson = Json.write(ctx);
    }

    /**
     * Advances tokens until each one is parked on something that needs the outside
     * world: a worker (READY), a clock (WAITING), an external actor (AWAITING), a
     * sibling (JOINED), or nothing at all (DONE at an END node).
     */
    private void drive(Tx tx, LazyGraph def, Instance inst, Deque<Token> work, long now) {
        int guard = 0;
        while (!work.isEmpty()) {
            if (++guard > 10_000) throw new IllegalStateException("cycle detected in workflow " + def.key());
            Token t = work.pop();
            Step s = new Step(tx, def, inst, t, def.node(t.nodeId), work, now);
            if (!NodeBehaviours.of(s.node().kind()).advance(this, s)) return;
        }
    }

    /**
     * Called whenever an instance reaches a terminal state: if it was a sub-workflow, resume (or
     * fail) the parent's waiting token. Lock ordering is always child -> parent, never the
     * reverse in one transaction, so parent/child completions cannot deadlock.
     */
    void notifyParent(Tx tx, Instance child, long now) {
        if (child.parentTokenId == null) return;
        Token probe = tx.findToken(child.parentTokenId).orElse(null);
        if (probe == null) return;
        Instance parent = tx.lockInstance(probe.instanceId).orElse(null);
        if (parent == null || parent.status != InstanceStatus.RUNNING) return;
        Token t = tx.findToken(child.parentTokenId).orElse(null);   // re-read under the lock
        if (t == null || t.status != TokenStatus.AWAITING || t.kind != NodeKind.SUB_WORKFLOW) return;
        LazyGraph def = definitions.graph(tx, parent.workflow, parent.version);
        Node node = def.node(t.nodeId);
        if (child.status != InstanceStatus.COMPLETED) {
            t.status = TokenStatus.FAILED;
            t.updatedAt = now;
            tx.updateToken(t);
            failInstance(tx, parent, "sub-workflow '" + node.activity() + "' " + child.status
                    + (child.error == null ? "" : ": " + child.error), now);
            return;
        }
        mergeContext(parent, Json.parse(child.contextJson));
        settleToken(tx, t, now);
        touchInstance(tx, parent, now);
        Token cont = newToken(parent, node.next(), t.joinStack, t.payloadJson, now);
        LOG.log(System.Logger.Level.DEBUG, () -> "sub-workflow " + child.id + " completed -> resuming parent "
                + parent.id + " at " + node.next());
        drive(tx, def, parent, new ArrayDeque<>(List.of(cont)), now);
    }

    LazyGraph def(Tx tx, Instance inst) {
        return definitions.graph(tx, inst.workflow, inst.version);
    }

    /** The context a worker sees. For a forEach item token it is the ITEM's working value itself;
     *  otherwise the shared instance context with the token's payload overlaid (minus the internal
     *  bookkeeping keys). */
    static Object dispatchContext(Instance inst, Token t) {
        if (t.payloadJson == null) return Json.parse(inst.contextJson);
        Map<String, Object> overlay = Json.parseObject(t.payloadJson);
        if (overlay.containsKey(ITEM_VALUE)) return overlay.get(ITEM_VALUE);
        Map<String, Object> ctx = Json.parseObject(inst.contextJson);
        overlay.remove(ARM_IDX);
        overlay.remove(LOOP_COUNTS);
        ctx.putAll(overlay);
        return ctx;
    }

    /** The frozen pre-forEach context an item step sees via {@code Step.base()}: the shared context
     *  plus any enclosing fork-branch overlay the forEach was spawned inside (minus bookkeeping). */
    private static Object itemBaseContext(Instance inst, Token t) {
        Map<String, Object> ctx = Json.parseObject(inst.contextJson);
        Map<String, Object> overlay = Json.parseObject(t.payloadJson);
        overlay.remove(ARM_IDX);
        overlay.remove(ITEM_VALUE);
        overlay.remove(ITEM_MAP_KEY);
        overlay.remove(LOOP_COUNTS);
        ctx.putAll(overlay);
        return ctx;
    }

    void failInstance(Tx tx, Instance inst, String error, long now) {
        cancelActiveTokens(tx, inst.id, now);
        if (sagas.begin(tx, inst, error, now)) return;
        inst.status = InstanceStatus.FAILED;
        inst.error = error;
        inst.updatedAt = now;
        tx.updateInstance(inst);
        LOG.log(System.Logger.Level.INFO, () -> "instance " + inst.id + " failed: " + error);
        notifyParent(tx, inst, now);
    }

    private static void cancelActiveTokens(Tx tx, String instanceId, long now) {
        for (Token t : tx.tokensOf(instanceId)) {
            if (!t.isActive() || t.id == null) continue;
            TokenStatus before = t.status;
            t.status = TokenStatus.CANCELLED;
            t.leaseOwner = null;
            t.leaseExpiresAt = 0;
            t.updatedAt = now;
            tx.updateToken(t);
            LOG.log(System.Logger.Level.DEBUG, () -> "cancelActiveTokens: " + instanceId + " token " + t.id
                    + " at " + t.nodeId + " " + before + " -> CANCELLED");
        }
    }

    static Token newToken(Instance inst, String nodeId, String joinStack, String payload, long now) {
        Token t = new Token();
        t.persisted = false;
        t.payloadJson = payload;
        t.id = Ids.next("tok");
        t.instanceId = inst.id;
        t.workflow = inst.workflow;
        t.version = inst.version;
        t.nodeId = nodeId;
        t.kind = NodeKind.TASK;
        t.status = TokenStatus.READY;
        t.attempt = 0;
        t.availableAt = now;
        t.joinStack = joinStack == null ? "" : joinStack;
        t.createdAt = now;
        t.updatedAt = now;
        return t;
    }
}
