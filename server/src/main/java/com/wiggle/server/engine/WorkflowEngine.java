package com.wiggle.server.engine;

import com.wiggle.core.Doc;
import com.wiggle.core.Ids;
import com.wiggle.core.InstanceView;
import com.wiggle.core.Node;
import com.wiggle.core.NodeKind;
import com.wiggle.core.TaskActivation;
import com.wiggle.core.WorkflowDefinition;
import com.wiggle.core.WorkflowVersion;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.Rows.Token;
import com.wiggle.server.store.Rows.TokenStatus;
import com.wiggle.server.store.Storage;
import com.wiggle.server.store.TokenPayload;
import com.wiggle.server.store.Tx;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * The state machine. Everything an instance does is expressed as tokens moving over
 * the compiled graph, in the spirit of a Petri net: a fork mints one token per branch,
 * a join consumes them, and the instance is terminal when no token is active.
 *
 * <p>The two state machines themselves live next door: {@link Instances} owns the
 * instance row, {@link Tokens} owns the token rows, and nothing else writes either.
 * What remains here is the work of joining them -- the drive pump that advances tokens until
 * they park, the worker-report entry points that apply a result under the instance lock, and
 * the leader sweeps -- plus the public surface the gRPC and cluster layers call.
 *
 * <p>All mutations for a given instance are serialised by {@link Tx#lockInstance}, which
 * is what lets several server nodes drive the same instance without stepping on
 * each other.
 */
public final class WorkflowEngine {

    private static final System.Logger LOG = System.getLogger(WorkflowEngine.class.getName());

    /** Default doWhile iteration budget for loops that don't declare one: a loop guard may
     *  evaluate true at most this many times before the instance FAILS with a clear error. An
     *  unbounded loop with a buggy condition is a self-inflicted denial of service — it hot-spins
     *  workers and the database and grows the instance's token rows without limit — so every
     *  doWhile is budgeted; a loop that legitimately needs more says so in the topology. */
    final long loopMaxIterations = ServerEnv.envLong("WIGGLE_LOOP_MAX_ITERATIONS", 10_000);

    private final DefinitionRegistry definitions;
    private final Queries queries;
    /** Who is polling this node and for what -- so the console can show unclaimable work. */
    private final PollerRegistry pollers = new PollerRegistry(60_000);
    /** Wake-on-produce for long-polling workers (Layer 1; see docs/in-memory-dispatch.md). */
    private final DispatchNotifier notifier = new DispatchNotifier();
    private final Transactions transactions;
    private final Instances instances;
    private final Dispatch dispatch;
    private final Schedules schedules;
    private final long defaultLeaseMillis;
    private final NodeBehaviourFactory nodeBehaviourFactory;

    public WorkflowEngine(Storage storage, DefinitionRegistry definitions, long defaultLeaseMillis) {
        this(storage, definitions, defaultLeaseMillis, () -> Ids.next("wfi"));
    }

    public WorkflowEngine(Storage storage, DefinitionRegistry definitions, long defaultLeaseMillis, InstanceIds idMinter) {
        this.definitions            = definitions;
        this.queries                = new Queries(storage, pollers);
        this.defaultLeaseMillis     = defaultLeaseMillis;
        this.transactions           = new Transactions(storage, notifier);
        var tokens                  = new Tokens(definitions, transactions::wake);
        this.instances              = new Instances(definitions, tokens, idMinter, this::drive);
        this.dispatch               = new Dispatch(transactions, tokens, notifier, pollers, defaultLeaseMillis);
        this.schedules              = new Schedules(transactions, instances);
        this.nodeBehaviourFactory   = new NodeBehaviourFactory(instances, tokens);
    }

    public DefinitionRegistry definitions() { return definitions; }

    public WorkflowDefinition register(WorkflowDefinition def) { return definitions.register(def); }

    /** {@link #register(WorkflowDefinition)}, optionally replacing an already-published version's
     *  graph. See {@link DefinitionRegistry#register(WorkflowDefinition, boolean)}. */
    public WorkflowDefinition register(WorkflowDefinition def, boolean force) {
        return definitions.register(def, force);
    }

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
        return transactions.inTx(tx -> instances.start(tx, workflow, version, context, correlationId, null));
    }

    public void cancel(String instanceId, String reason) {
        List<String> children = transactions.inTx(tx -> instances.cancel(tx, instanceId, reason));
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
        return poll(workerId, queues, max, leaseMillis, deadline, Cancellation.never());
    }

    public List<TaskActivation> poll(String workerId, Set<String> queues, int max, Long leaseMillis, long deadline,
                                     Cancellation cancelled) {
        return poll(workerId, queues, null, max, leaseMillis, deadline, cancelled);
    }

    /** Long-polls for work; see {@link Dispatch#poll}. */
    public List<TaskActivation> poll(String workerId, Set<String> queues, Set<WorkflowVersion> versions, int max,
                                     Long leaseMillis, long deadline,
                                     Cancellation cancelled) {
        return dispatch.poll(workerId, queues, versions, max, leaseMillis, deadline, cancelled);
    }

    /** Extends the lease of an in-flight task (worker heartbeat for long-running steps). */
    public long extendLease(String taskId, String leaseOwner, long extraMillis) {
        long until = transactions.read(tx -> Tokens.extendLease(tx, taskId, leaseOwner, extraMillis));
        LOG.log(System.Logger.Level.DEBUG, () ->
                "extendLease: task " + taskId + " owner=" + leaseOwner + " now expires at " + until);
        return until;
    }

    /**
     * Completes a task. For TASK nodes (combines included) {@code result} REPLACES the context —
     * the handler's return is the complete next context, sent whole by the worker; a null result
     * leaves the context untouched. For PREDICATE nodes it must carry a boolean under
     * {@code "value"}.
     */
    public void complete(String taskId, String leaseOwner, Object result) {
        transactions.inTxVoid(tx -> complete0(taskId, leaseOwner, result, tx));
    }

    private void complete0(String taskId, String leaseOwner, Object result, Tx tx) {
        Tokens.LockedTask locked = Tokens.lock(tx, taskId);
        Instance inst = locked.inst();
        Token t = locked.token();
        Tokens.requireLease(t, leaseOwner);
        long now = System.currentTimeMillis();
        Long compSeq = Sagas.seqOf(t);
        if (compSeq != null) {
            instances.compensatorCompleted(tx, inst, t, compSeq, now);
            return;
        }
        Instances.requireRunning(inst);
        LazyGraph def = definitions.graph(tx, t.workflow, t.version);
        Node node = def.node(t.nodeId);
        Doc compInput = node.compensable() ? Scopes.dispatchContext(inst, t) : null;
        NodeBehaviour behaviour = nodeBehaviourFactory.getNodeBehaviour(node.kind());
        String next = behaviour.route(inst, t, node, result);
        if (node.compensable()) Sagas.capture(tx, inst, t, node, compInput, now);
        String overrun = behaviour.overrunAfter(this, t, node, result);
        if (overrun != null) {
            Tokens.settle(tx, t, now);
            instances.fail(tx, inst, overrun, now);
            return;
        }
        Tokens.settle(tx, t, now);
        Instances.touch(tx, inst, now);
        Token cont = Tokens.continueAt(tx, inst, t, next,
                Scopes.stripCombineScratch(node, t.payload), now);
        drive(tx, def, inst, new ArrayDeque<>(List.of(cont)), now);
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
        if (steps.isEmpty()) throw EngineException.badRequest("advance requires at least one step");
        return transactions.inTx(tx -> advance0(startTaskId, leaseOwner, steps, finalHandback, tx));
    }

    private @NonNull AdvanceOutcome advance0(String startTaskId, String leaseOwner, List<StepInput> steps, boolean finalHandback, Tx tx) {
        Tokens.LockedTask locked = Tokens.lock(tx, startTaskId);
        Instance inst = locked.inst();
        long now = System.currentTimeMillis();
        long leaseExpiry = now + defaultLeaseMillis;
        if (!InstanceState.of(inst.status).running()) {
            return new AdvanceOutcome(inst.status.name(), 0, null);
        }
        Tokens.requireLease(locked.token(), leaseOwner);
        LazyGraph def = definitions.graph(tx, inst.workflow, inst.version);
        return doAdvance(tx, def, inst, locked.token(), leaseOwner, steps, finalHandback, now, leaseExpiry);
    }

    private AdvanceOutcome doAdvance(Tx tx, LazyGraph def, Instance inst, Token current, String leaseOwner,
                                     List<StepInput> steps, boolean finalHandback, long now, long leaseExpiry) {
        String nextTaskId = null;
        for (int i = 0; i < steps.size(); i++) {
            StepInput step = steps.get(i);
            Node node = def.node(current.nodeId);
            requireMatchingNode(node, step, current);
            NodeBehaviour behaviour = nodeBehaviourFactory.getNodeBehaviour(node.kind());
            Doc compInput = node.compensable() ? Scopes.dispatchContext(inst, current) : null;
            String next = behaviour.routeReported(inst, current, node, step);
            if (node.compensable()) Sagas.capture(tx, inst, current, node, compInput, now);
            String overrun = behaviour.overrunReported(this, current, node, step);
            if (overrun != null) {
                Tokens.settle(tx, current, now);
                instances.fail(tx, inst, overrun, now);
                return new AdvanceOutcome(inst.status.name(), 0, null);
            }
            Tokens.settle(tx, current, now);
            Token cont = Tokens.create(inst, next, current.joinStack,
                    Scopes.stripCombineScratch(node, current.payload), now);
            Node nextNode = def.node(next);
            boolean lastStep = i == steps.size() - 1;
            if ((lastStep && finalHandback) || !nextNode.isWorkerDispatched()) {
                Instances.touch(tx, inst, now);
                handBack(tx, def, inst, cont, nextNode, now);
                return new AdvanceOutcome(inst.status.name(), leaseExpiry, null);
            }
            Tokens.createLeased(tx, cont, nextNode, leaseOwner, leaseExpiry, now);
            LOG.log(System.Logger.Level.DEBUG, () -> "advanceRun: instance " + inst.id
                    + " chaining locally " + node.name() + " -> " + next);
            current = cont;
            nextTaskId = cont.id;
        }
        Instances.touch(tx, inst, now);
        return new AdvanceOutcome(inst.status.name(), leaseExpiry, nextTaskId);
    }

    private static void requireMatchingNode(Node node, StepInput step, Token current) {
        if (!node.id().equals(step.nodeId())) {
            throw EngineException.conflict("reported step " + step.nodeId() + " but token "
                    + current.id + " is at " + node.id());
        }
    }

    /** Hand back: drive the continuation normally (READY for a worker, or a boundary). */
    private void handBack(Tx tx, LazyGraph def, Instance inst, Token cont, Node nextNode, long now) {
        tx.insertToken(cont);
        LOG.log(System.Logger.Level.DEBUG, () -> "advanceRun: instance " + inst.id
                + " handing back at " + cont.nodeId + " (" + nextNode.kind() + ")");
        drive(tx, def, inst, new ArrayDeque<>(List.of(cont)), now);
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
        transactions.inTxVoid(tx -> {
            Instance inst = tx.lockInstance(instanceId).orElseThrow(() -> EngineException.notFound("instance"));
            Instances.requireRunning(inst);
            Token t = tx.tokensOf(instanceId).stream()
                    .filter(x -> x.status == TokenStatus.AWAITING && x.kind == NodeKind.SIGNAL)
                    .filter(x -> name.equals(x.activity))
                    .findFirst()
                    .orElseThrow(() -> EngineException.conflict(
                            "instance " + instanceId + " is not waiting for signal '" + name + "'"));
            long now = System.currentTimeMillis();
            LazyGraph def = definitions.graph(tx, t.workflow, t.version);
            Node node = def.node(t.nodeId);
            TokenPayload contPayload = Scopes.mergeIntoScope(inst, t.payload, payload);
            Tokens.settle(tx, t, now);
            Instances.touch(tx, inst, now);
            Token cont = Tokens.continueAt(tx, inst, t, node.next(), contPayload, now);
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
                transactions.inTxVoid(tx -> action.apply(tx, token));
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
        List<Token> due = transactions.read(tx -> tx.dueTimers(System.currentTimeMillis(), max));
        logDue("fireDueTimers", due);
        return sweep(due, "timer", this::fireTimer);
    }

    private void fireTimer(Tx tx, Token timer) {
        Instance inst = tx.lockInstance(timer.instanceId).orElse(null);
        if (inst == null || !InstanceState.of(inst.status).running()) return;
        Token t = tx.findToken(timer.id).orElse(null);
        if (t == null || t.status != TokenStatus.WAITING) return;
        long ts = System.currentTimeMillis();
        LazyGraph def = definitions.graph(tx, t.workflow, t.version);
        Node node = def.node(t.nodeId);
        Tokens.settle(tx, t, ts);
        Token cont = Tokens.continueAt(tx, inst, t, node.next(), t.payload, ts);
        LOG.log(System.Logger.Level.DEBUG, () -> "timer " + node.name()
                + " of instance " + inst.id + " fired -> " + node.next());
        drive(tx, def, inst, new ArrayDeque<>(List.of(cont)), ts);
    }

    /** Leader duty: signal waits whose deadline has passed escalate (to {@code altNext}) or fail. */
    public int fireDueSignalDeadlines(int max) {
        List<Token> due = transactions.read(tx -> tx.dueSignals(System.currentTimeMillis(), max));
        logDue("fireDueSignalDeadlines", due);
        return sweep(due, "signal deadline", this::escalateOrFailSignal);
    }

    private void escalateOrFailSignal(Tx tx, Token task) {
        Instance inst = tx.lockInstance(task.instanceId).orElse(null);
        if (inst == null || !InstanceState.of(inst.status).running()) return;
        Token t = tx.findToken(task.id).orElse(null);
        if (t == null || t.status != TokenStatus.AWAITING) return;
        long ts = System.currentTimeMillis();
        if (t.availableAt <= 0 || t.availableAt > ts) return;   // deadline cleared or moved
        LazyGraph def = definitions.graph(tx, t.workflow, t.version);
        Node node = def.node(t.nodeId);
        Tokens.settle(tx, t, ts);
        if (node.altNext() == null) {
            LOG.log(System.Logger.Level.DEBUG, () -> "signal " + node.name()
                    + " of instance " + inst.id + " missed its deadline, no escalation -> failing instance");
            instances.fail(tx, inst, "signal '" + node.name() + "' timed out", ts);
            return;
        }
        Token cont = Tokens.continueAt(tx, inst, t, node.altNext(), t.payload, ts);
        LOG.log(System.Logger.Level.DEBUG, () -> "signal " + node.name()
                + " of instance " + inst.id + " missed its deadline -> escalating to " + node.altNext());
        drive(tx, def, inst, new ArrayDeque<>(List.of(cont)), ts);
    }

    /** Leader duty: return tasks whose worker died back to the ready pool. */
    public int reclaimExpiredLeases(int max) {
        List<Token> orphans = transactions.read(tx -> tx.expiredLeases(System.currentTimeMillis(), max));
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
        settleFailure(tx, inst, t, node, "lease expired (worker unreachable)", "lease expired", true, ts);
    }

    /** Fails a task. Retries per the node's policy; when exhausted the whole instance fails. */
    public void fail(String taskId, String leaseOwner, String message, boolean retryable) {
        transactions.inTxVoid(tx -> {
            Tokens.LockedTask locked = Tokens.lock(tx, taskId);
            Instance inst = locked.inst();
            Token t = locked.token();
            Tokens.requireLease(t, leaseOwner);
            // COMPENSATING instances still have live work in flight — their compensators. A
            // compensator's failure report must reach the retry-or-fail transition
            // (-> COMPENSATION_FAILED), not be dropped by the terminal-status guard.
            if (!InstanceState.of(inst.status).live()) return;
            Node node = definitions.graph(tx, t.workflow, t.version).node(t.nodeId);
            settleFailure(tx, inst, t, node, message, message, retryable, System.currentTimeMillis());
        });
    }

    /**
     * Hands a token failure to {@link Tokens#reportFailure} and applies what
     * the verdict means
     * for the instance: nothing while retries remain, the comp-log for an exhausted compensator, and
     * otherwise the instance itself (as {@code node.name() + ": " + failReason}).
     */
    private void settleFailure(Tx tx, Instance inst, Token t, Node node,
                               String lastError, String failReason, boolean retryable, long now) {
        Tokens.Outcome outcome = Tokens.reportFailure(tx, t, node, lastError, failReason, retryable, now);
        if (!(outcome instanceof Tokens.Outcome.Exhausted(String reason, Long compSeq))) return;
        if (compSeq != null) {
            instances.compensatorExhausted(tx, inst, node, compSeq, reason, now);
            return;
        }
        if (InstanceState.of(inst.status).running()) {
            instances.fail(tx, inst, node.name() + ": " + reason, now);
        }
    }

    /** Creates a recurring start: {@code workflow} fires every {@code every}, first fire after one interval. */
    public String createSchedule(String workflow, java.time.Duration every, Object context) {
        return schedules.every(workflow, every, context);
    }

    /** Creates a recurring start on a five-field cron expression (evaluated in UTC). */
    public String createCronSchedule(String workflow, String cron, Object context) {
        return schedules.cron(workflow, cron, context);
    }

    public void deleteSchedule(String id) {
        schedules.delete(id);
    }

    public List<Rows.Schedule> schedules() {
        return schedules.all();
    }

    /** Leader duty: start instances for schedules whose fire time has passed. */
    public int fireDueSchedules(int max) {
        return schedules.fireDue(max);
    }

    public int purgeTerminalInstancesOlderThan(long retentionMillis, int max) {
        long cutoff = System.currentTimeMillis() - retentionMillis;
        int purged = transactions.read(tx -> Instances.purgeTerminalBefore(tx, cutoff, max));
        if (purged > 0) {
            LOG.log(System.Logger.Level.DEBUG, () -> "purgeTerminalInstancesOlderThan: removed " + purged
                    + " instance(s) updated before " + cutoff);
        }
        return purged;
    }

    /**
     * Advances tokens until each one is parked on something that needs the outside
     * world: a worker (READY), a clock (WAITING), an external actor (AWAITING), a
     * sibling (JOINED), or nothing at all (DONE at an END node).
     */
    private void drive(Tx tx, LazyGraph def, Instance inst, Deque<Token> work, long now) {
        // Caps chain DEPTH, not fan-out breadth: every extra child a fork/forEach enqueues beyond
        // the usual single continuation grows the budget by one, so any fan-out width advances
        // fine while a runaway chain of server-side nodes still trips the cap.
        long budget = 10_000;
        long guard = 0;
        while (!work.isEmpty()) {
            if (++guard > budget) {
                throw new IllegalStateException("drive budget exceeded in workflow " + def.key()
                        + ": " + guard + " advances without parking (runaway server-side node chain)");
            }
            Token t = work.pop();
            int before = work.size();
            Step s = new Step(tx, def, inst, t, def.node(t.nodeId), work, now);
            if (!nodeBehaviourFactory.getNodeBehaviour(s.node().kind()).advance(s)) return;
            budget += Math.max(0, work.size() - before - 1);
        }
    }
}
