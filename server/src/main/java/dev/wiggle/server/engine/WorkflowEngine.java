package dev.wiggle.server.engine;

import dev.wiggle.core.*;
import dev.wiggle.server.store.Rows;
import dev.wiggle.server.store.Rows.Instance;
import dev.wiggle.server.store.Rows.InstanceStatus;
import dev.wiggle.server.store.Rows.Token;
import dev.wiggle.server.store.Rows.TokenStatus;
import dev.wiggle.server.store.Storage;
import dev.wiggle.server.store.Tx;

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

    private final Storage storage;
    private final DefinitionRegistry definitions;
    private final long defaultLeaseMillis;

    public WorkflowEngine(Storage storage, DefinitionRegistry definitions, long defaultLeaseMillis) {
        this.storage = storage;
        this.definitions = definitions;
        this.defaultLeaseMillis = defaultLeaseMillis;
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

    // ------------------------------------------------------------- lifecycle

    public String start(String workflow, Integer version, Object context, String correlationId) {
        return storage.inTx(tx -> startInTx(tx, workflow, version, context, correlationId, null));
    }

    /** Starts an instance inside an existing transaction; {@code parentTokenId} links a sub-workflow. */
    private String startInTx(Tx tx, String workflow, Integer version, Object context,
                             String correlationId, String parentTokenId) {
        int v = version != null ? version : tx.latestVersion(workflow).orElseThrow(
                () -> EngineException.notFound("workflow '" + workflow + "'"));
        LazyGraph def = definitions.graph(tx, workflow, v);
        long now = System.currentTimeMillis();
        Instance inst = insertNewInstance(tx, def, context, correlationId, parentTokenId, now);
        Token t = newToken(inst, def.startNode(), "", null, now);
        tx.insertToken(t);
        LOG.log(System.Logger.Level.DEBUG, () -> "start: instance " + inst.id + " of " + def.key()
                + " at node " + def.startNode() + " correlationId=" + correlationId);
        drive(tx, def, inst, new ArrayDeque<>(List.of(t)), now);
        return inst.id;
    }

    private static Instance insertNewInstance(Tx tx, LazyGraph def, Object context, String correlationId,
                                              String parentTokenId, long now) {
        Instance inst = new Instance();
        inst.id = Ids.next("wfi");
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
        List<String> children = storage.inTx(tx -> {
            Instance inst = tx.lockInstance(instanceId).orElseThrow(() -> EngineException.notFound("instance"));
            if (inst.status != InstanceStatus.RUNNING) {
                LOG.log(System.Logger.Level.DEBUG, () ->
                        "cancel: instance " + instanceId + " ignored, already " + inst.status);
                return List.of();
            }
            long now = System.currentTimeMillis();
            cancelActiveTokens(tx, inst.id, null, now);
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
        return storage.inTx(tx -> tx.findInstance(id).map(WorkflowEngine::view));
    }

    public List<InstanceView> list(String workflow, String status, int limit) {
        InstanceStatus s = status == null ? null : InstanceStatus.valueOf(status.toUpperCase(Locale.ROOT));
        return storage.inTx(tx -> tx.listInstances(workflow, s, limit).stream().map(WorkflowEngine::view).toList());
    }

    public List<Token> tokens(String instanceId) {
        return storage.inTx(tx -> tx.tokensOf(instanceId));
    }

    /** Snapshot of the dispatchable backlog right now: how many tasks are queued and waiting. */
    public Rows.QueueDepth queueDepth() {
        return storage.inTx(tx -> tx.queueDepth(System.currentTimeMillis()));
    }

    /**
     * Worker-dispatched tasks completed since {@code since}, across every node in the cluster --
     * the consumption-rate signal for lag monitoring.
     */
    public int tasksProcessedSince(long since) {
        return storage.inTx(tx -> tx.countProcessedSince(since));
    }

    private static InstanceView view(Instance i) {
        return new InstanceView(i.id, i.workflow, i.version, i.status.name(), i.terminationReason,
                i.error, Json.parse(i.contextJson), i.createdAt, i.updatedAt);
    }

    // ---------------------------------------------------------------- polling

    /** Leases up to {@code max} tasks for a worker. Returns immediately; long-polling lives in the HTTP layer. */
    public List<TaskActivation> poll(String workerId, Set<String> queues, int max, Long leaseMillis) {
        long now = System.currentTimeMillis();
        long lease = leaseMillis == null || leaseMillis <= 0 ? defaultLeaseMillis : leaseMillis;
        long until = now + lease;
        List<TaskActivation> out = storage.inTx(tx -> claimActivations(tx, workerId, queues, max, now, until));
        if (!out.isEmpty()) {
            LOG.log(System.Logger.Level.DEBUG, () -> "poll: worker " + workerId + " queues=" + queues
                    + " claimed " + out.size() + " task(s): "
                    + out.stream().map(a -> a.taskId() + "@" + a.stepName()).toList());
        }
        return out;
    }

    private List<TaskActivation> claimActivations(Tx tx, String workerId, Set<String> queues,
                                                  int max, long now, long until) {
        List<Token> claimed = tx.claimTasks(workerId, queues, max, now, until);
        List<TaskActivation> activations = new ArrayList<>(claimed.size());
        for (Token t : claimed) {
            activationFor(tx, t, workerId, until).ifPresent(activations::add);
        }
        return activations;
    }

    private Optional<TaskActivation> activationFor(Tx tx, Token t, String workerId, long until) {
        Instance inst = tx.findInstance(t.instanceId).orElse(null);
        if (inst == null || inst.status != InstanceStatus.RUNNING) return Optional.empty();
        Node node = definitions.graph(tx, t.workflow, t.version).node(t.nodeId);
        ExecutionMode mode = resolveMode(definitions.executionMode(tx, t.workflow, t.version));
        return Optional.of(new TaskActivation(t.id, inst.id, inst.workflow, inst.version, node.id(), node.name(),
                node.activity(), node.kind(), t.attempt + 1, until, workerId, dispatchContext(inst, t), mode));
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

    // ------------------------------------------------------------ completion

    /** A task's token re-read under its instance's write lock. */
    private record LockedTask(Instance inst, Token token) {}

    /** Takes the instance lock first, then re-reads the token under it. */
    private static LockedTask lockTask(Tx tx, String taskId) {
        Token probe = tx.findToken(taskId).orElseThrow(() -> EngineException.notFound("task"));
        Instance inst = tx.lockInstance(probe.instanceId).orElseThrow(() -> EngineException.notFound("instance"));
        Token token = tx.findToken(taskId).orElseThrow(() -> EngineException.notFound("task"));
        return new LockedTask(inst, token);
    }

    private static void requireRunning(Instance inst) {
        if (inst.status != InstanceStatus.RUNNING) {
            throw EngineException.conflict("instance " + inst.id + " is " + inst.status);
        }
    }

    /**
     * Completes a task. For TASK nodes {@code result} is shallow-merged into the instance
     * context; for PREDICATE nodes it must carry a boolean under {@code "value"}.
     */
    public void complete(String taskId, String leaseOwner, Object result) {
        storage.inTxVoid(tx -> {
            LockedTask locked = lockTask(tx, taskId);
            Instance inst = locked.inst();
            Token t = locked.token();
            requireLease(t, leaseOwner);
            requireRunning(inst);
            long now = System.currentTimeMillis();
            LazyGraph def = definitions.graph(tx, t.workflow, t.version);
            Node node = def.node(t.nodeId);
            String next = routeCompletion(inst, node, result);
            settleToken(tx, t, now);
            touchInstance(tx, inst, now);
            Token cont = newToken(inst, next, t.joinStack, t.payloadJson, now);
            tx.insertToken(cont);
            drive(tx, def, inst, new ArrayDeque<>(List.of(cont)), now);
        });
    }

    /** Merges a task result (or routes a predicate) and returns the successor node id. */
    private static String routeCompletion(Instance inst, Node node, Object result) {
        if (node.kind() != NodeKind.PREDICATE) {
            mergeContext(inst, result);
            LOG.log(System.Logger.Level.DEBUG, () -> "complete: task " + node.name()
                    + " of instance " + inst.id + " done -> " + node.next());
            return node.next();
        }
        boolean value = predicateValue(result);
        String next = GraphTraversal.successor(node, value);
        LOG.log(System.Logger.Level.DEBUG, () -> "complete: predicate " + node.name()
                + " of instance " + inst.id + " evaluated " + value + " -> " + next);
        return next;
    }

    /** Marks a token consumed and releases its lease. */
    private static void settleToken(Tx tx, Token t, long now) {
        t.status = TokenStatus.DONE;
        t.leaseOwner = null;
        t.leaseExpiresAt = 0;
        t.updatedAt = now;
        tx.updateToken(t);
    }

    private static void touchInstance(Tx tx, Instance inst, long now) {
        inst.updatedAt = now;
        tx.updateInstance(inst);
    }

    // ------------------------------------------------------ local execution

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
    public AdvanceOutcome advanceRun(String startTaskId, String leaseOwner, List<StepInput> steps, boolean finalHandback) {
        return storage.inTx(tx -> {
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
            String next = routeReportedStep(inst, node, step);
            settleToken(tx, current, now);
            touchInstance(tx, inst, now);
            Token cont = newToken(inst, next, current.joinStack, current.payloadJson, now);
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

    private static String routeReportedStep(Instance inst, Node node, StepInput step) {
        if (node.kind() == NodeKind.PREDICATE) {
            boolean value = step.predicateValue() != null && step.predicateValue();
            return GraphTraversal.successor(node, value);
        }
        mergeContext(inst, step.merge());
        return node.next();
    }

    /** Hand back: drive the continuation normally (READY for a worker, or a boundary). */
    private void handBack(Tx tx, LazyGraph def, Instance inst, Token cont, Node nextNode, long now) {
        tx.insertToken(cont);
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
        tx.insertToken(cont);
    }

    // ---------------------------------------------------------------- signals

    /** The signal waits currently pending an external delivery, oldest first. */
    public List<Token> pendingSignals(int max) {
        return storage.inTx(tx -> tx.pendingSignals(max));
    }

    /**
     * Delivers a named signal to an instance. The instance must currently be waiting on that
     * signal (there is no buffering; an early signal is a conflict the sender can retry).
     * {@code payload} merges into the context and the flow advances down the signal's path.
     */
    public void signal(String instanceId, String name, Object payload) {
        storage.inTxVoid(tx -> {
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
            tx.insertToken(cont);
            LOG.log(System.Logger.Level.DEBUG, () -> "signal: '" + name + "' delivered to instance "
                    + inst.id + " -> " + node.next());
            drive(tx, def, inst, new ArrayDeque<>(List.of(cont)), now);
        });
    }

    // --------------------------------------------------------- housekeeping

    /** A per-token action executed in its own transaction by a leader sweep. */
    private interface SweepAction {
        void apply(Tx tx, Token token);
    }

    /** Runs {@code action} once per token, each in its own transaction, isolating failures. */
    private int sweep(List<Token> due, String what, SweepAction action) {
        int done = 0;
        for (Token token : due) {
            try {
                storage.inTxVoid(tx -> action.apply(tx, token));
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
        tx.insertToken(cont);
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
        tx.insertToken(cont);
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
        storage.inTxVoid(tx -> {
            LockedTask locked = lockTask(tx, taskId);
            Instance inst = locked.inst();
            Token t = locked.token();
            requireLease(t, leaseOwner);
            if (inst.status != InstanceStatus.RUNNING) return;
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
        if (inst.status == InstanceStatus.RUNNING) {
            failInstance(tx, inst, node.name() + ": " + failReason, now);
        }
    }

    // -------------------------------------------------------------- schedules

    /** Creates a recurring start: {@code workflow} fires every {@code every}, first fire after one interval. */
    public String createSchedule(String workflow, java.time.Duration every, Object context) {
        if (every.toMillis() < 1) throw EngineException.badRequest("schedule interval must be positive");
        return storage.inTx(tx -> {
            tx.latestVersion(workflow).orElseThrow(
                    () -> EngineException.notFound("workflow '" + workflow + "'"));
            Rows.Schedule s = new Rows.Schedule();
            s.id = Ids.next("sched");
            s.workflow = workflow;
            s.intervalMillis = every.toMillis();
            s.contextJson = Json.write(context == null ? Map.of() : context);
            s.nextFireAt = System.currentTimeMillis() + s.intervalMillis;
            s.createdAt = System.currentTimeMillis();
            tx.putSchedule(s);
            LOG.log(System.Logger.Level.INFO, () -> "schedule " + s.id + ": " + workflow
                    + " every " + s.intervalMillis + "ms");
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
        return storage.inTx(tx -> {
            if (!tx.claimSchedule(sched.id, sched.nextFireAt, now + sched.intervalMillis)) return false;
            String id = startInTx(tx, sched.workflow, null, Json.parse(sched.contextJson),
                    "schedule:" + sched.id, null);
            LOG.log(System.Logger.Level.DEBUG, () -> "schedule " + sched.id + " fired -> instance " + id);
            return true;
        });
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

    // ------------------------------------------------------------- helpers

    private static boolean predicateValue(Object result) {
        if (result instanceof Boolean b) return b;
        if (result instanceof Map<?, ?> m && m.get("value") instanceof Boolean b) return b;
        throw EngineException.badRequest("predicate result must be a boolean or {\"value\": <boolean>}");
    }

    private static void requireLease(Token t, String leaseOwner) {
        if (t.status != TokenStatus.RUNNING) {
            throw EngineException.conflict("task " + t.id + " is " + t.status + ", not RUNNING");
        }
        if (leaseOwner != null && !leaseOwner.equals(t.leaseOwner)) {
            throw EngineException.conflict("lease for task " + t.id + " is held by " + t.leaseOwner);
        }
    }

    @SuppressWarnings("unchecked")
    private static void mergeContext(Instance inst, Object result) {
        if (result == null) return;
        if (!(result instanceof Map)) {
            inst.contextJson = Json.write(result);
            return;
        }
        Map<String, Object> ctx = Json.parseObject(inst.contextJson);
        ctx.putAll((Map<String, Object>) result);
        inst.contextJson = Json.write(ctx);
    }

    // ------------------------------------------------------ the state machine

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
            Node node = def.node(t.nodeId);
            boolean keepDriving = switch (node.kind()) {
                case TASK, PREDICATE -> parkAtWorkerStep(tx, inst, t, node, now);
                case SLEEP -> parkAtSleep(tx, inst, t, node, now);
                case SIGNAL -> parkAtSignal(tx, inst, t, node, now);
                case SUB_WORKFLOW -> launchSubWorkflow(tx, inst, t, node, now);
                case FORK -> spawnForkBranches(tx, inst, t, node, work, now);
                case DYN_FORK -> spawnDynamicBranches(tx, inst, t, node, work, now);
                case JOIN -> arriveAtJoin(tx, inst, t, node, work, now);
                case END -> finishAtEnd(tx, inst, t, node, work, now);
            };
            if (!keepDriving) return;
        }
    }

    private boolean parkAtWorkerStep(Tx tx, Instance inst, Token t, Node node, long now) {
        TokenStatus before = t.status;
        t.status = TokenStatus.READY;
        t.kind = node.kind();
        t.activity = node.activity();
        t.queue = node.queue();
        t.availableAt = now;
        t.updatedAt = now;
        tx.updateToken(t);
        LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                + node.name() + " (" + node.kind() + ") " + before + " -> READY, queue=" + node.queue());
        return true;
    }

    private boolean parkAtSleep(Tx tx, Instance inst, Token t, Node node, long now) {
        TokenStatus before = t.status;
        t.status = TokenStatus.WAITING;
        t.kind = NodeKind.SLEEP;
        t.availableAt = now + node.sleepMillis();
        t.updatedAt = now;
        tx.updateToken(t);
        LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                + node.name() + " (SLEEP) " + before + " -> WAITING until " + t.availableAt
                + " (" + node.sleepMillis() + "ms)");
        return true;
    }

    /**
     * Parks until the named signal arrives. No worker leases an AWAITING token; a positive
     * availableAt is the (optional) deadline the leader sweeps.
     */
    private boolean parkAtSignal(Tx tx, Instance inst, Token t, Node node, long now) {
        TokenStatus before = t.status;
        t.status = TokenStatus.AWAITING;
        t.kind = NodeKind.SIGNAL;
        t.activity = node.name();     // the signal's name, matched by signal()
        t.availableAt = node.sleepMillis() > 0 ? now + node.sleepMillis() : 0;
        t.updatedAt = now;
        tx.updateToken(t);
        LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                + node.name() + " (SIGNAL) " + before + " -> AWAITING, deadline="
                + (t.availableAt > 0 ? t.availableAt : "none"));
        return true;
    }

    /**
     * Starts a child instance of the workflow named by the node and parks this token until the
     * child reaches a terminal state ({@link #notifyParent}). The child's input is the parent's
     * context (with any branch payload overlaid); an unregistered child workflow fails the parent.
     */
    private boolean launchSubWorkflow(Tx tx, Instance inst, Token t, Node node, long now) {
        TokenStatus before = t.status;
        t.status = TokenStatus.AWAITING;
        t.kind = NodeKind.SUB_WORKFLOW;
        t.activity = node.activity();   // the child workflow's name
        t.availableAt = 0;
        t.updatedAt = now;
        tx.updateToken(t);
        String childId;
        try {
            childId = startInTx(tx, node.activity(), null, dispatchContext(inst, t), "sub:" + t.id, t.id);
        } catch (EngineException e) {
            failInstance(tx, inst, "sub-workflow '" + node.activity() + "': " + e.getMessage(), now);
            return false;
        }
        LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                + node.name() + " (SUB_WORKFLOW) " + before + " -> AWAITING child " + childId);
        return true;
    }

    /**
     * Called whenever an instance reaches a terminal state: if it was a sub-workflow, resume (or
     * fail) the parent's waiting token. Lock ordering is always child -> parent, never the
     * reverse in one transaction, so parent/child completions cannot deadlock.
     */
    private void notifyParent(Tx tx, Instance child, long now) {
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
        tx.insertToken(cont);
        LOG.log(System.Logger.Level.DEBUG, () -> "sub-workflow " + child.id + " completed -> resuming parent "
                + parent.id + " at " + node.next());
        drive(tx, def, parent, new ArrayDeque<>(List.of(cont)), now);
    }

    private boolean spawnForkBranches(Tx tx, Instance inst, Token t, Node node, Deque<Token> work, long now) {
        TokenStatus before = t.status;
        t.status = TokenStatus.DONE;
        t.kind = NodeKind.FORK;
        t.updatedAt = now;
        tx.updateToken(t);
        String group = t.id;   // unique per fork execution; the join finds the fork token by it
        String childStack = t.pushJoinStack(group);
        for (String branchStart : node.branches()) {
            Token child = newToken(inst, branchStart, childStack, t.payloadJson, now);
            tx.insertToken(child);
            work.push(child);
        }
        LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                + node.name() + " (FORK) " + before + " -> DONE, spawned " + node.branches().size()
                + " branch(es) in group " + group + ": " + node.branches());
        return true;
    }

    /**
     * Runtime fan-out: one child per element of the list at the node's {@code itemsKey}, each
     * carrying its element (and index) as a branch-scoped payload. The join group encodes the
     * width, since a dynamic join's expected count varies per execution. An empty or missing
     * list skips straight past the paired join; a non-list value fails the instance.
     */
    private boolean spawnDynamicBranches(Tx tx, Instance inst, Token t, Node node, Deque<Token> work, long now) {
        Object items = Json.parseObject(inst.contextJson).get(node.itemsKey());
        if (items != null && !(items instanceof List)) {
            failInstance(tx, inst, "forkEach '" + node.name() + "': context key '" + node.itemsKey()
                    + "' holds " + items.getClass().getSimpleName() + ", not a list", now);
            return false;
        }
        List<?> list = items == null ? List.of() : (List<?>) items;
        TokenStatus before = t.status;
        t.status = TokenStatus.DONE;
        t.kind = NodeKind.DYN_FORK;
        t.updatedAt = now;
        tx.updateToken(t);
        if (list.isEmpty()) {
            // Nothing to fan out over: continue directly past the paired join (node.next()).
            Token cont = newToken(inst, def(tx, inst).node(node.next()).next(), t.joinStack, t.payloadJson, now);
            tx.insertToken(cont);
            work.push(cont);
            LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                    + node.name() + " (DYN_FORK) " + before + " -> DONE, empty '" + node.itemsKey()
                    + "' skips the join");
            return true;
        }
        String group = t.id + "#" + list.size();   // fork token id + width, parsed back at the join
        String childStack = t.pushJoinStack(group);
        String branchStart = node.branches().get(0);
        for (int i = 0; i < list.size(); i++) {
            Token child = newToken(inst, branchStart, childStack, itemPayload(t, node, list.get(i), i), now);
            tx.insertToken(child);
            work.push(child);
        }
        LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                + node.name() + " (DYN_FORK) " + before + " -> DONE, spawned " + list.size()
                + " branch(es) over '" + node.itemsKey() + "' in group " + group);
        return true;
    }

    /** The child's payload: the fork token's own payload (nesting) plus its item and index. */
    private static String itemPayload(Token forkToken, Node node, Object item, int index) {
        Map<String, Object> payload = forkToken.payloadJson == null
                ? new LinkedHashMap<>() : Json.parseObject(forkToken.payloadJson);
        payload.put(node.itemKey(), item);
        payload.put(node.itemKey() + "Index", (long) index);
        return Json.write(payload);
    }

    private LazyGraph def(Tx tx, Instance inst) {
        return definitions.graph(tx, inst.workflow, inst.version);
    }

    private boolean arriveAtJoin(Tx tx, Instance inst, Token t, Node node, Deque<Token> work, long now) {
        String group = t.currentJoinGroup();
        int expected = expectedAt(node, group);
        TokenStatus before = t.status;
        t.status = TokenStatus.JOINED;
        t.kind = NodeKind.JOIN;
        t.updatedAt = now;
        tx.updateToken(t);
        List<Token> atBarrier = joinedAtBarrier(tx, inst, node, group);
        if (atBarrier.size() < expected) {
            LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                    + node.name() + " (JOIN) " + before + " -> JOINED, waiting on barrier " + group
                    + " (" + atBarrier.size() + "/" + expected + ")");
            return true;
        }
        consumeBarrier(tx, atBarrier, now);
        // Restore the payload the branches started from, so nesting scopes correctly.
        Token cont = newToken(inst, node.next(), t.popJoinStack(), forkPayload(tx, group), now);
        tx.insertToken(cont);
        work.push(cont);
        LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                + node.name() + " (JOIN) " + before + " -> JOINED, barrier " + group
                + " satisfied (" + atBarrier.size() + "/" + expected + ") -> " + node.next());
        return true;
    }

    /** A static join's width comes from the graph; a dynamic one travels in the group as "#n". */
    private static int expectedAt(Node node, String group) {
        if (node.expected() > 0) return node.expected();
        int hash = group == null ? -1 : group.lastIndexOf('#');
        return hash < 0 ? 1 : Integer.parseInt(group.substring(hash + 1));
    }

    /** The payload the fork token had when it spawned this group, or null for legacy groups. */
    private static String forkPayload(Tx tx, String group) {
        if (group == null) return null;
        int hash = group.lastIndexOf('#');
        String forkTokenId = hash < 0 ? group : group.substring(0, hash);
        return tx.findToken(forkTokenId).map(f -> f.payloadJson).orElse(null);
    }

    /** The context a worker sees: the shared instance context with the token's payload overlaid. */
    private static Object dispatchContext(Instance inst, Token t) {
        if (t.payloadJson == null) return Json.parse(inst.contextJson);
        Map<String, Object> ctx = Json.parseObject(inst.contextJson);
        ctx.putAll(Json.parseObject(t.payloadJson));
        return ctx;
    }

    private static List<Token> joinedAtBarrier(Tx tx, Instance inst, Node node, String group) {
        return tx.tokensOf(inst.id).stream()
                .filter(x -> x.status == TokenStatus.JOINED)
                .filter(x -> node.id().equals(x.nodeId))
                .filter(x -> Objects.equals(group, x.currentJoinGroup()))
                .toList();
    }

    /**
     * Consumes the barrier: these tokens have served their purpose, and leaving them parked
     * would keep the instance looking active forever.
     */
    private static void consumeBarrier(Tx tx, List<Token> atBarrier, long now) {
        for (Token parked : atBarrier) {
            parked.status = TokenStatus.DONE;
            parked.updatedAt = now;
            tx.updateToken(parked);
        }
    }

    private boolean finishAtEnd(Tx tx, Instance inst, Token t, Node node, Deque<Token> work, long now) {
        TokenStatus before = t.status;
        t.status = TokenStatus.DONE;
        t.kind = NodeKind.END;
        t.updatedAt = now;
        tx.updateToken(t);
        if (!node.success()) {
            LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                    + node.name() + " (END) " + before + " -> DONE, unsuccessful end -> failing instance");
            failInstance(tx, inst, node.reason() == null ? "terminated" : node.reason(), now);
            return false;
        }
        boolean anyActive = tx.tokensOf(inst.id).stream().anyMatch(Token::isActive);
        if (anyActive || !work.isEmpty()) {
            LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                    + node.name() + " (END) " + before + " -> DONE, other tokens still active");
            return true;
        }
        inst.status = InstanceStatus.COMPLETED;
        inst.terminationReason = node.reason();
        inst.updatedAt = now;
        tx.updateInstance(inst);
        LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                + node.name() + " (END) " + before + " -> DONE, no tokens remain -> instance COMPLETED"
                + (node.reason() != null ? " (" + node.reason() + ")" : ""));
        notifyParent(tx, inst, now);
        return true;
    }

    private void failInstance(Tx tx, Instance inst, String error, long now) {
        cancelActiveTokens(tx, inst.id, null, now);
        inst.status = InstanceStatus.FAILED;
        inst.error = error;
        inst.updatedAt = now;
        tx.updateInstance(inst);
        LOG.log(System.Logger.Level.INFO, () -> "instance " + inst.id + " failed: " + error);
        notifyParent(tx, inst, now);
    }

    private static void cancelActiveTokens(Tx tx, String instanceId, String except, long now) {
        for (Token t : tx.tokensOf(instanceId)) {
            if (!t.isActive() || t.id.equals(except)) continue;
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

    private static Token newToken(Instance inst, String nodeId, String joinStack, String payload, long now) {
        Token t = new Token();
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
