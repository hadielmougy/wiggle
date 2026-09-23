package com.wiggle.server.engine;

import com.wiggle.core.*;
import com.wiggle.server.store.*;
import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.Rows.Token;
import com.wiggle.server.store.Rows.TokenStatus;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
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
    private final long loopMaxIterations = ServerEnv.envLong("WIGGLE_LOOP_MAX_ITERATIONS", 10_000);

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
    /** Built once: the factory reads the engine's collaborators lazily, per create(). */
    private final RunningModeFactory modeFactory = new DefaultModeFactory();

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

    class DefaultModeFactory extends RunningModeFactory {
        @Override Instances instances() { return instances;}
        @Override NodeBehaviourFactory nodeBehaviourFactory() { return nodeBehaviourFactory;}
        @Override DefinitionRegistry definitions() {return definitions;}
    }

    /**
     * Completes a task. For TASK nodes (combines included) {@code result} REPLACES the context —
     * the handler's return is the complete next context, sent whole by the worker; a null result
     * leaves the context untouched. For PREDICATE nodes it must carry a boolean under
     * {@code "value"}.
     */
    public void complete(String taskId, String leaseOwner, Object result) {
        complete(taskId, leaseOwner, result, null, null, List.of());
    }

    /** {@link #complete(String, String, Object)} with the handler's own start and finish, which
     *  replace the server's claimed-to-settled stamps on the token. */
    public void complete(String taskId, String leaseOwner, Object result, Long startedAt, Long finishedAt) {
        complete(taskId, leaseOwner, result, startedAt, finishedAt, List.of());
    }

    /** {@link #complete(String, String, Object, Long, Long)} with the events the handler emitted,
     *  appended to the log in the transaction that settles the step. */
    public void complete(String taskId, String leaseOwner, Object result, Long startedAt, Long finishedAt,
                         List<EmittedEvent> events) {
        transactions.inTxVoid(tx -> {
            Tokens.LockedTask task = Tokens.lock(tx, taskId);
            Tokens.requireLease(task.token(), leaseOwner);
            if (compensated(tx, task, events)) return;
            ExecutionMode mode = definitions.executionMode(tx, task.inst().workflow, task.inst().version);
            modeFactory.create(mode)
                    .execute(new CompleteExecutionContext(task, leaseOwner, result, tx, loopMaxIterations, startedAt, finishedAt, events));
        });
    }

    private boolean compensated(Tx tx, Tokens.LockedTask task, List<EmittedEvent> events) {
        Long compSeq = Sagas.seqOf(task.token());
        if (compSeq != null) {
            long now = System.currentTimeMillis();
            Events.emitted(tx, task.inst(), task.token().nodeId, events, now);
            instances.compensatorCompleted(tx, task.inst(), task.token(), compSeq, now);
            return true;
        }
        return false;
    }

    /**
     * One step reported after it ran: a task's complete next context, a predicate's value, or the
     * error it threw (observed runs only). {@code startedAt}/{@code finishedAt} are the step's own
     * clock in epoch millis, or null when the reporter did not time it.
     */
    public record StepInput(String nodeId, Object merge, Boolean predicateValue, String error,
                            Long startedAt, Long finishedAt, List<EmittedEvent> events) {

        public StepInput {
            events = events == null ? List.of() : List.copyOf(events);
        }

        public StepInput(String nodeId, Object merge, Boolean predicateValue) {
            this(nodeId, merge, predicateValue, null, null, null, List.of());
        }

        public StepInput(String nodeId, Object merge, Boolean predicateValue, String error,
                         Long startedAt, Long finishedAt) {
            this(nodeId, merge, predicateValue, error, startedAt, finishedAt, List.of());
        }
    }

    /** The result of applying a reported run: the instance's status, renewed lease, and next token. */
    public record AdvanceOutcome(String instanceStatus, long leaseExpiresAt, String nextTaskId) {}

    /** One run of a cross-instance batch: exactly the arguments of {@link #advance}. */
    public record Run(String startTaskId, String leaseOwner, List<StepInput> steps, boolean finalHandback) {}

    /**
     * A run's fate in a batch: the {@link AdvanceOutcome} the single-run path would have
     * returned, or the status and message it would have thrown. A rejected run wrote nothing
     * and may be reported again.
     */
    public record RunResult(AdvanceOutcome outcome, Integer errorStatus, String error) {

        public boolean ok() {
            return outcome != null;
        }

        static RunResult of(AdvanceOutcome outcome) {
            return new RunResult(outcome, null, null);
        }

        static RunResult reject(EngineException e) {
            return new RunResult(null, e.statusCode(), e.getMessage());
        }
    }

    public AdvanceOutcome advance(Run run) {
        if (run.steps.isEmpty()) throw EngineException.badRequest("advance requires at least one step");
        return transactions.inTx(tx -> {
            Tokens.LockedTask task = Tokens.lock(tx, run.startTaskId);
            ExecutionMode mode = definitions.executionMode(tx, task.inst().workflow, task.inst().version);
            return modeFactory.create(mode)
                    .execute(new AdvanceRunContext(task, run.leaseOwner, run.steps, run.finalHandback, tx, loopMaxIterations, defaultLeaseMillis));
        });
    }

    public Map<String, RunResult> advanceMany(List<Run> runs) {
        requireWellFormed(runs);
        try {
            return transactions.inTx(tx -> modeFactory.create(ExecutionMode.LOCAL_ASYNC)
                    .execute(new AdvanceBatchContext(runs, tx, loopMaxIterations, defaultLeaseMillis)));
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, () -> "advanceMany: batch of " + runs.size()
                    + " rolled back (" + e + "); replaying each run in its own transaction");
            return replaySingly(runs);
        }
    }

    private static void requireWellFormed(List<Run> runs) {
        if (runs.isEmpty()) throw EngineException.badRequest("advanceMany requires at least one run");
        Set<String> ids = new HashSet<>();
        for (Run run : runs) {
            if (run.steps().isEmpty()) throw EngineException.badRequest("advance requires at least one step");
            if (!ids.add(run.startTaskId())) {
                throw EngineException.badRequest("task " + run.startTaskId() + " appears twice in the batch");
            }
        }
    }

    /**
     * The pathological path: one run per transaction, so the broken run fails alone. Every run
     * gets a result whatever happens -- earlier replays have already committed, so throwing out
     * of this loop would tell the caller nothing happened when some of it durably did.
     */
    private Map<String, RunResult> replaySingly(List<Run> runs) {
        Map<String, RunResult> results = new LinkedHashMap<>();
        for (Run run : runs) {
            try {
                results.put(run.startTaskId(), RunResult.of(advance(run)));
            } catch (EngineException e) {
                results.put(run.startTaskId(), RunResult.reject(e));
            } catch (RuntimeException e) {
                results.put(run.startTaskId(), new RunResult(null, 500, e.toString()));
            }
        }
        return results;
    }

    /** How long after END, or a final report, an observed run waits for stragglers before it is judged. */
    private final long observeSettleMillis = ServerEnv.envLong("wiggle.observe.settleMillis", "WIGGLE_OBSERVE_SETTLE_MILLIS", 5_000);
    /** How long an event stays in the log past its append, once every consumer has acknowledged it. */
    private final long eventRetentionMillis = ServerEnv.envLong("wiggle.events.retentionMillis", "WIGGLE_EVENTS_RETENTION_MILLIS", 7L * 24 * 3_600_000);
    /** How long an appended event is held back from the feed, covering appends still in flight. */
    private final long eventVisibilityMillis = ServerEnv.envLong("wiggle.events.visibilityMillis", "WIGGLE_EVENTS_VISIBILITY_MILLIS", 50);
    /** How long an observed run may go without a report before it is judged as stalled. */
    private final long observeStallMillis = ServerEnv.envLong("wiggle.observe.stallMillis", "WIGGLE_OBSERVE_STALL_MILLIS", 600_000);

    /**
     * Appends a run of steps an instrumented application already executed (OBSERVED execution)
     * to the run {@code correlationId} names, creating it on first sight; a blank key mints one,
     * for a run only this reporter will ever report. Nothing is judged here -- the settle sweep
     * does that once the run has gone quiet -- so the only refusals are a workflow that is not
     * OBSERVED or does not exist.
     */
    public ObserveResult observe(String workflow, Integer version, String instanceId, String correlationId,
                                 String reporter, List<StepInput> steps, boolean fin) {
        if (steps.isEmpty() && !fin) throw EngineException.badRequest("observe requires at least one step");
        if (reporter == null || reporter.isBlank()) throw EngineException.badRequest("observe requires a reporter");
        String key = correlationId == null || correlationId.isBlank() ? Ids.token() : correlationId;
        return transactions.inTx(tx -> {
            Instance inst;
            if (instanceId != null && !instanceId.isBlank()) {
                inst = tx.lockInstance(instanceId).orElseThrow(() -> EngineException.notFound("instance"));
                ObservedRunningMode.requireObserved(
                        definitions.executionMode(tx, inst.workflow, inst.version), inst.workflow + ":" + inst.version);
            } else {
                inst = instances.observedRun(tx, workflow, version, key);
            }
            return modeFactory.create(ExecutionMode.OBSERVED).execute(
                    new ObserveRunContext(inst, reporter, steps, fin, tx, observeSettleMillis, observeStallMillis));
        });
    }

    /** Leader duty: judge observed runs whose settle time has passed. */
    public int settleObservedRuns(int max) {
        List<Instance> due = transactions.read(tx -> tx.dueSettle(System.currentTimeMillis(), max));
        int done = 0;
        for (Instance probe : due) {
            try {
                transactions.inTxVoid(tx -> settleObservedRun(tx, probe.id));
                done++;
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.WARNING, "settle of observed run " + probe.id + " failed: " + e);
            }
        }
        return done;
    }

    private void settleObservedRun(Tx tx, String id) {
        Instance inst = tx.lockInstance(id).orElse(null);
        long now = System.currentTimeMillis();
        if (inst == null || !InstanceState.of(inst.status).running() || inst.settleAt == null || inst.settleAt > now) return;
        WorkflowDefinition def = definitions.lookup(inst.workflow, inst.version)
                .orElseThrow(() -> EngineException.notFound("workflow '" + inst.workflow + ":" + inst.version + "'"));
        boolean idle = inst.settleAt - inst.updatedAt > observeSettleMillis;
        modeFactory.create(ExecutionMode.OBSERVED).execute(new SettleContext(tx, inst, def, idle, now));
        LOG.log(System.Logger.Level.DEBUG, () -> "settled observed run " + inst.id + " -> " + inst.status
                + (idle ? " (idle)" : ""));
    }

    /** One run of an observe batch: exactly the arguments of {@link #observe}. */
    public record ObservedRun(String workflow, Integer version, String instanceId, String correlationId,
                              String reporter, List<StepInput> steps, boolean fin) {}

    /** A run's fate in an observe batch: its result, or the status and message it would have thrown. */
    public record ObserveOutcome(ObserveResult result, Integer errorStatus, String error) {

        public boolean ok() {
            return result != null;
        }
    }

    /**
     * Applies N observed runs, each in its own transaction, and answers every one in submission
     * order. Observation needs no atomicity across runs: a run that is refused is refused alone,
     * and the ones around it stand.
     */
    public List<ObserveOutcome> observeMany(List<ObservedRun> runs) {
        List<ObserveOutcome> out = new ArrayList<>(runs.size());
        for (ObservedRun r : runs) {
            try {
                out.add(new ObserveOutcome(observe(r.workflow(), r.version(), r.instanceId(), r.correlationId(),
                        r.reporter(), r.steps(), r.fin()), null, null));
            } catch (EngineException e) {
                out.add(new ObserveOutcome(null, e.statusCode(), e.getMessage()));
            } catch (RuntimeException e) {
                out.add(new ObserveOutcome(null, 500, e.toString()));
            }
        }
        return out;
    }

    /**
     * Per-node duration statistics for one workflow version over its newest {@code sample} timed
     * steps that finished after {@code since}; a null or zero version means the latest. Slowest
     * 95th percentile first, so the head of the list is the bottleneck.
     */
    public List<NodeStats> stepStats(String workflow, Integer version, long since, int sample) {
        WorkflowDefinition def = (version == null || version <= 0
                ? definitions.latest(workflow) : definitions.lookup(workflow, version))
                .orElseThrow(() -> EngineException.notFound("workflow '" + workflow + "'"));
        List<Rows.StepDuration> durations = transactions.read(
                tx -> tx.stepDurations(def.name(), def.version(), since, sample));
        return StepStatistics.summarise(durations, id -> {
            Node n = def.nodes().get(id);
            return n == null ? id : n.name();
        });
    }

    /** Departures of observed runs from their topology, newest first; either filter may be null. */
    /** Up to {@code max} entries of the event log after {@code afterSeq}, oldest first. */
    public List<EventView> events(long afterSeq, int max) {
        return view(transactions.read(tx -> tx.eventsAfter(afterSeq, max)));
    }

    /**
     * Serves one consumer's next events and leaves its cursor where it was: delivery is
     * at-least-once, and only {@link #ackEvents} moves the cursor on. The first poll of a
     * consumer registers it, at the tail ({@code startFrom} 0), at the earliest event still
     * retained ({@code -1}), or after a seq it names; later polls ignore {@code startFrom}.
     *
     * <p>Long-polls until {@code deadline} the way a worker poll does, and never serves an event
     * younger than the visibility window, so a consumer cannot read past an append still in flight.
     */
    public List<EventView> pollEvents(String consumer, int max, long startFrom, long deadline,
                                      Cancellation cancelled) {
        String name = requireConsumer(consumer);
        int limit = max > 0 ? max : 1;
        long from = transactions.read(tx -> cursorSeq(tx, name, startFrom, System.currentTimeMillis()));
        long interval = Math.max(10, Math.min(eventVisibilityMillis, 200));
        while (true) {
            if (cancelled.cancelled()) return List.of();
            long visibleBefore = System.currentTimeMillis() - eventVisibilityMillis;
            List<Rows.Event> batch = transactions.read(tx -> tx.eventsAfter(from, visibleBefore, limit));
            if (!batch.isEmpty()) {
                LOG.log(System.Logger.Level.DEBUG, () -> "pollEvents: consumer " + name + " served "
                        + batch.size() + " event(s) after seq " + from);
                return view(batch);
            }
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) return List.of();
            try {
                Thread.sleep(Math.min(interval, remaining));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return List.of();
            }
        }
    }

    /**
     * Acknowledges every event up to {@code ackedSeq} for one consumer and returns where its
     * cursor now stands. Cumulative and never backwards, so a replayed ack is harmless; an ack
     * past the log's head is clamped to it, since nothing beyond has been delivered.
     */
    public long ackEvents(String consumer, long ackedSeq) {
        String name = requireConsumer(consumer);
        return transactions.read(tx -> {
            long target = Math.max(0, Math.min(ackedSeq, tx.latestEventSeq()));
            tx.advanceEventCursor(name, target, System.currentTimeMillis());
            Rows.EventCursor cursor = tx.eventCursor(name);
            return cursor == null ? target : cursor.ackedSeq();
        });
    }

    /** The seq a consumer resumes after: its cursor, registered at {@code startFrom} on a first poll. */
    private static long cursorSeq(Tx tx, String consumer, long startFrom, long now) {
        Rows.EventCursor cursor = tx.eventCursor(consumer);
        if (cursor != null) return cursor.ackedSeq();
        long from = startFrom == 0 ? tx.latestEventSeq() : Math.max(0, startFrom);
        tx.createEventCursorIfAbsent(new Rows.EventCursor(consumer, from, now, now));
        Rows.EventCursor created = tx.eventCursor(consumer);
        return created == null ? from : created.ackedSeq();
    }

    private static String requireConsumer(String consumer) {
        if (consumer == null || consumer.isBlank()) {
            throw EngineException.badRequest("a consumer name is required: the feed's cursor is named, "
                    + "so two consumers with no name would share one place in the log");
        }
        return consumer;
    }

    private static List<EventView> view(List<Rows.Event> events) {
        return events.stream()
                .map(e -> new EventView(e.seq(), e.instanceId(), e.workflow(), e.version(), e.correlationId(),
                        e.type(), e.nodeId(), e.createdAt(),
                        e.payload() == null ? Map.of() : Json.parseObject(e.payload())))
                .toList();
    }

    /**
     * Drops events older than the retention window that every consumer has acknowledged (all of
     * them, while no consumer exists). Leader-only, from the housekeeper's retention sweep.
     */
    public int trimEvents(int max) {
        long cutoff = System.currentTimeMillis() - eventRetentionMillis;
        int trimmed = transactions.read(tx -> tx.deleteEvents(cutoff, tx.oldestAckedSeq(), max));
        if (trimmed > 0) LOG.log(System.Logger.Level.DEBUG, () -> "trimEvents: removed " + trimmed + " event(s)");
        return trimmed;
    }

    public List<AnomalyView> anomalies(String workflow, String instanceId, int limit) {
        return transactions.read(tx -> tx.anomalies(workflow, instanceId, limit)).stream()
                .map(a -> new AnomalyView(a.instanceId(), a.workflow(), a.version(), a.kind(),
                        a.expectedNode(), a.reportedNode(), a.detail(), a.at()))
                .toList();
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
        Drive.pump(nodeBehaviourFactory, tx, def, inst, work, now);
    }
}
