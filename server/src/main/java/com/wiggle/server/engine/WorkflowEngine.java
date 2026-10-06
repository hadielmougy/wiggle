package com.wiggle.server.engine;

import com.wiggle.core.*;
import com.wiggle.server.store.BufferedTx;
import com.wiggle.server.store.*;
import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.Rows.LockedTask;
import com.wiggle.server.store.Rows.Token;
import com.wiggle.core.TokenStatus;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

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

    /** A retry backing off at least this long waits WAITING until {@link #promoteDueRetries}, not READY. */
    private final long retryParkFromMillis = ServerEnv.envLong(
            "wiggle.retry.timerMinMillis", "WIGGLE_RETRY_TIMER_MIN_MILLIS", 1_000);

    /** How many of a leader sweep's due items run at once, each in its own transaction. */
    private final Sweeper sweeper = new Sweeper((int) ServerEnv.envLong(
            "wiggle.sweep.parallelism", "WIGGLE_SWEEP_PARALLELISM", 4));

    private final DefinitionRegistry definitions;
    private final Queries queries;
    /** Who is polling this node and for what -- so the console can show unclaimable work. */
    private final PollerRegistry pollers = new PollerRegistry(60_000);
    /** Wake-on-produce for long-polling workers (Layer 1; see docs/in-memory-dispatch.md). */
    private final DispatchNotifier notifier = new DispatchNotifier();
    private final Transactions transactions;
    private final Instances instances;
    private final InstanceIds idMinter;
    private final Tokens tokens;
    private final Dispatch dispatch;
    private final Schedules schedules;
    private final long defaultLeaseMillis;
    private final NodeBehaviourFactory nodeBehaviourFactory;
    private final StepChain stepChain;
    private final LocalAsyncBatch localAsyncBatch;

    public WorkflowEngine(Storage storage, DefinitionRegistry definitions, long defaultLeaseMillis) {
        this(storage, definitions, defaultLeaseMillis, InstanceIds.onShard(0));
    }

    public WorkflowEngine(Storage storage, DefinitionRegistry definitions, long defaultLeaseMillis, InstanceIds idMinter) {
        this.definitions            = definitions;
        this.queries                = new Queries(storage, pollers);
        this.defaultLeaseMillis     = defaultLeaseMillis;
        this.transactions           = new Transactions(storage, notifier);
        this.tokens                 = new Tokens(definitions, transactions::wake);
        this.idMinter               = idMinter;
        this.instances              = new Instances(definitions, tokens, idMinter, this::drive);
        this.dispatch               = new Dispatch(transactions, tokens, notifier, pollers, defaultLeaseMillis);
        this.schedules              = new Schedules(transactions, instances, sweeper);
        this.nodeBehaviourFactory   = new NodeBehaviourFactory(instances, tokens);
        this.stepChain              = new StepChain(instances, nodeBehaviourFactory, definitions, loopMaxIterations, defaultLeaseMillis,
                Spawns.Limits.fromEnv());
        this.localAsyncBatch        = new LocalAsyncBatch(stepChain, definitions);
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
        String id = idMinter.next();
        return transactions.inTx(id, raw -> buffered(raw,
                tx -> instances.start(tx, id, workflow, version, context, correlationId, null)));
    }

    public void cancel(String instanceId, String reason) {
        List<String> children = transactions.inTx(instanceId, tx -> instances.cancel(tx, instanceId, reason));
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
        return extendLease(taskId, leaseOwner, extraMillis, null);
    }

    /** The same, for a caller that serves only the queues {@code queues} accepts (null: every queue). */
    public long extendLease(String taskId, String leaseOwner, long extraMillis, Predicate<String> queues) {
        long until = transactions.read(taskId, tx -> Tokens.extendLease(tx, taskId, leaseOwner, extraMillis, queues));
        LOG.log(System.Logger.Level.DEBUG, () ->
                "extendLease: task " + taskId + " owner=" + leaseOwner + " now expires at " + until);
        return until;
    }

    /**
     * A compensator reports through the same call as any other step, but it is not a step of the
     * flow: it closes one entry of the reverse pass, and its instance is COMPENSATING rather than
     * RUNNING, so it never reaches the step chain.
     */
    private boolean compensated(Tx tx, LockedTask task, Run run) {
        Long compSeq = Sagas.seqOf(task.token());
        if (compSeq == null) return false;
        if (run.steps().size() != 1) {
            throw EngineException.badRequest("a compensator reports exactly one step");
        }
        StepInput step = run.steps().getFirst();
        Tokens.requireLease(task.token(), run.leaseOwner());
        StepChain.requireMatchingNode(task.token(), step);
        long now = System.currentTimeMillis();
        Events.emitted(tx, task.inst(), task.token().nodeId, step.events(), now);
        instances.compensatorCompleted(tx, task.inst(), task.token(), compSeq, now);
        return true;
    }

    /**
     * One step reported after it ran: a task's complete next context, or a predicate's value.
     * {@code startedAt}/{@code finishedAt} are the step's own clock in epoch millis, or null when the
     * reporter did not time it.
     */
    public record StepInput(String nodeId, Object merge, Boolean predicateValue,
                            Long startedAt, Long finishedAt, List<EmittedEvent> events,
                            List<CreatedBranch> branches) {

        public StepInput {
            events = events == null ? List.of() : List.copyOf(events);
            branches = branches == null ? List.of() : List.copyOf(branches);
        }

        public StepInput(String nodeId, Object merge, Boolean predicateValue,
                         Long startedAt, Long finishedAt, List<EmittedEvent> events) {
            this(nodeId, merge, predicateValue, startedAt, finishedAt, events, List.of());
        }

        public StepInput(String nodeId, Object merge, Boolean predicateValue) {
            this(nodeId, merge, predicateValue, null, null, List.of());
        }

        public StepInput(String nodeId, Object merge, Boolean predicateValue, Long startedAt, Long finishedAt) {
            this(nodeId, merge, predicateValue, startedAt, finishedAt, List.of());
        }
    }

    /** One run of a cross-instance batch: exactly the arguments of {@link #report}. */
    public record Run(String startTaskId, String leaseOwner, List<StepInput> steps, boolean finalHandback) {}

    /**
     * A run's fate in a batch: the {@link ReportOutcome} the single-run path would have
     * returned, or the status and message it would have thrown. A rejected run wrote nothing
     * and may be reported again.
     */
    public record RunResult(ReportOutcome outcome, Integer errorStatus, String error) {

        public boolean ok() {
            return outcome != null;
        }

        static RunResult of(ReportOutcome outcome) {
            return new RunResult(outcome, null, null);
        }

        static RunResult reject(EngineException e) {
            return new RunResult(null, e.statusCode(), e.getMessage());
        }
    }

    /**
     * The one way a worker reports finished work: one step, or the ordered run of steps it chained
     * locally. The workflow's declared mode decides whether the continuation is leased back to this
     * worker or released, so the caller cannot choose it and cannot choose it wrong. A mode that
     * never chains applies every reported step in order and then hands the continuation back.
     */
    public ReportOutcome report(Run run) {
        return report(run, null);
    }

    /** The same, for a caller that serves only the queues {@code queues} accepts (null: every queue). */
    public ReportOutcome report(Run run, Predicate<String> queues) {
        if (run.steps.isEmpty()) throw EngineException.badRequest("report requires at least one step");
        return transactions.inTx(run.startTaskId, raw -> buffered(raw, tx -> {
            LockedTask task = Tokens.lock(tx, run.startTaskId);
            Tokens.requireQueue(task.token(), queues);
            ExecutionMode mode = definitions.executionMode(tx, task.inst().workflow, task.inst().version);
            if (compensated(tx, task, run)) {
                return new ReportOutcome(task.inst().status.name(), 0, null);
            }
            return stepChain.apply(tx, task, run, ExecutionModes.chainsBack(mode));
        }));
    }

    /**
     * The same report, for one run or several. A lone run is applied on its own and carries no
     * batching restrictions, since there is nothing to batch it with; several are admitted only
     * where batching is sound and commit together. Either way every submitted run gets an answer,
     * and a refused one wrote nothing and may be reported again in a call of its own.
     */
    public Map<String, RunResult> report(List<Run> runs) {
        return report(runs, null);
    }

    /** The same, for a caller that serves only the queues {@code queues} accepts (null: every queue). */
    public Map<String, RunResult> report(List<Run> runs, Predicate<String> queues) {
        requireWellFormed(runs);
        if (runs.size() == 1) return replaySingly(runs, queues);
        Map<Integer, List<Run>> byShard = new LinkedHashMap<>();
        for (Run run : runs) {
            byShard.computeIfAbsent(transactions.shardOf(run.startTaskId()), s -> new ArrayList<>()).add(run);
        }
        if (byShard.size() > 1) {
            Map<String, RunResult> merged = new HashMap<>();
            byShard.values().forEach(group -> merged.putAll(report(group, queues)));
            Map<String, RunResult> results = new LinkedHashMap<>();
            for (Run run : runs) results.put(run.startTaskId(), merged.get(run.startTaskId()));
            return results;
        }
        int shard = byShard.keySet().iterator().next();
        try {
            return transactions.inShard(shard, tx -> localAsyncBatch.apply(tx, runs, queues));
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, () -> "reportSteps: batch of " + runs.size()
                    + " rolled back (" + e + "); replaying each run in its own transaction");
            return replaySingly(runs, queues);
        }
    }

    private static void requireWellFormed(List<Run> runs) {
        if (runs.isEmpty()) throw EngineException.badRequest("report requires at least one run");
        Set<String> ids = new HashSet<>();
        for (Run run : runs) {
            if (run.steps().isEmpty()) throw EngineException.badRequest("report requires at least one step");
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
    private Map<String, RunResult> replaySingly(List<Run> runs, Predicate<String> queues) {
        Map<String, RunResult> results = new LinkedHashMap<>();
        for (Run run : runs) {
            try {
                results.put(run.startTaskId(), RunResult.of(report(run, queues)));
            } catch (EngineException e) {
                results.put(run.startTaskId(), RunResult.reject(e));
            } catch (RuntimeException e) {
                results.put(run.startTaskId(), new RunResult(null, 500, e.toString()));
            }
        }
        return results;
    }

    /** How long an event stays in the log past its append, once every consumer has acknowledged it. */
    private final long eventRetentionMillis = ServerEnv.envLong("wiggle.events.retentionMillis", "WIGGLE_EVENTS_RETENTION_MILLIS", 7L * 24 * 3_600_000);
    /** How long an appended event is held back from the feed, covering appends still in flight. */
    private final long eventVisibilityMillis = ServerEnv.envLong("wiggle.events.visibilityMillis", "WIGGLE_EVENTS_VISIBILITY_MILLIS", 50);

    /** Whether a leader sweep should act on this item: its instance is still there, and still
     *  running the forward flow. A swept item whose instance moved on is dropped, not failed. */
    private static boolean sweepable(Instance inst) {
        return inst != null && inst.status.running();
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
        List<Rows.StepDuration> durations = transactions.readEach(
                tx -> tx.stepDurations(def.name(), def.version(), since, sample));
        return StepStatistics.summarise(durations, id -> {
            Node n = def.nodes().get(id);
            return n == null ? id : n.name();
        });
    }

    /** Up to {@code max} entries of the event log after {@code afterSeq} on every shard, oldest first. */
    public List<EventView> events(long afterSeq, int max) {
        List<List<Rows.Event>> perShard = new ArrayList<>();
        List<Integer> shards = transactions.instanceShards();
        for (int shard : shards) perShard.add(transactions.readShard(shard, tx -> tx.eventsAfter(afterSeq, max)));
        List<EventView> out = new ArrayList<>();
        merge(shards, perShard, max, (shard, e) -> out.add(view(e, shard, null)));
        return out;
    }

    /**
     * Serves one consumer's next events and leaves its cursor where it was: delivery is
     * at-least-once, and only {@link #ackEvents} moves the cursor on. The first poll of a
     * consumer registers it: at the tail of every shard's log ({@code startFrom} 0), at the
     * earliest entry each still retains ({@code -1}), or, on one shard, after a seq it names;
     * later polls ignore {@code startFrom}.
     *
     * <p>Every shard keeps its own log; a poll reads each beyond the consumer's position there and
     * merges them oldest first, keeping each shard's own order. Each served entry carries the
     * cursor that acknowledges it and everything served before it.
     *
     * <p>Long-polls until {@code deadline} the way a worker poll does, and never serves an event
     * younger than the visibility window, so a consumer cannot read past an append still in flight.
     */
    public List<EventView> pollEvents(String consumer, int max, long startFrom, long deadline,
                                      Cancellation cancelled) {
        String name = requireConsumer(consumer);
        int limit = max > 0 ? max : 1;
        List<Integer> shards = transactions.instanceShards();
        FeedCursor from = cursorOf(name, startFrom, shards);
        long interval = Math.max(10, Math.min(eventVisibilityMillis, 200));
        while (true) {
            if (cancelled.cancelled()) return List.of();
            long visibleBefore = System.currentTimeMillis() - eventVisibilityMillis;
            List<List<Rows.Event>> perShard = new ArrayList<>();
            for (int shard : shards) {
                long after = from.at(shard);
                perShard.add(transactions.readShard(shard, tx -> tx.eventsAfter(after, visibleBefore, limit)));
            }
            List<EventView> batch = new ArrayList<>();
            FeedCursor[] running = {from};
            merge(shards, perShard, limit, (shard, e) -> {
                running[0] = running[0].with(shard, e.seq());
                batch.add(view(e, shard, running[0].format()));
            });
            if (!batch.isEmpty()) {
                LOG.log(System.Logger.Level.DEBUG, () -> "pollEvents: consumer " + name + " served "
                        + batch.size() + " event(s) after " + from.format());
                return batch;
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
     * Merges each shard's events, oldest first, into {@code sink}, up to {@code max}. Each shard's list
     * is in seq order and stays so: the merge always takes the head of some shard's list.
     */
    private static void merge(List<Integer> shards, List<List<Rows.Event>> perShard, int max,
                              java.util.function.BiConsumer<Integer, Rows.Event> sink) {
        int[] next = new int[shards.size()];
        for (int served = 0; served < max; served++) {
            int pick = -1;
            for (int i = 0; i < shards.size(); i++) {
                if (next[i] >= perShard.get(i).size()) continue;
                if (pick < 0 || perShard.get(i).get(next[i]).createdAt()
                        < perShard.get(pick).get(next[pick]).createdAt()) pick = i;
            }
            if (pick < 0) return;
            sink.accept(shards.get(pick), perShard.get(pick).get(next[pick]++));
        }
    }

    /**
     * The consumer's place: its acknowledged position on every shard, registered at {@code startFrom}
     * on a first poll. A shard the consumer has no position on yet -- one added after it registered --
     * is read from its start.
     */
    private FeedCursor cursorOf(String consumer, long startFrom, List<Integer> shards) {
        int home = transactions.home();
        FeedCursor existing = transactions.readHome(tx -> stored(tx, consumer, shards, home));
        if (existing != null) return existing;
        if (startFrom > 0 && shards.size() > 1) {
            throw EngineException.badRequest("startFrom names one seq, but every shard keeps its own log; "
                    + "register at the tail (0) or the earliest entry (-1)");
        }
        Map<Integer, Long> start = new HashMap<>();
        for (int shard : shards) {
            long latest = transactions.readShard(shard, Tx::latestEventSeq);
            start.put(shard, startFrom == 0 ? latest : Math.max(0, startFrom));
        }
        long now = System.currentTimeMillis();
        return transactions.readHome(tx -> {
            tx.createEventCursorIfAbsent(new Rows.EventCursor(consumer, start.getOrDefault(home, 0L), now, now));
            for (int shard : shards) {
                if (shard != home) tx.advanceEventPosition(consumer, shard, start.get(shard));
            }
            return stored(tx, consumer, shards, home);
        });
    }

    /** The consumer's stored positions on {@code shards}, or null when it has never polled. */
    private static FeedCursor stored(Tx tx, String consumer, List<Integer> shards, int home) {
        Rows.EventCursor cursor = tx.eventCursor(consumer);
        if (cursor == null) return null;
        Map<Integer, Long> others = tx.eventPositions(consumer);
        java.util.SortedMap<Integer, Long> at = new java.util.TreeMap<>();
        for (int shard : shards) at.put(shard, shard == home ? cursor.ackedSeq() : others.getOrDefault(shard, 0L));
        return new FeedCursor(at);
    }

    /**
     * Acknowledges every event up to {@code cursor} -- the cursor of the last event handled -- for one
     * consumer, and returns where it now stands. Cumulative and never backwards on any shard, so a
     * replayed ack is harmless; a position past a shard's head is clamped to it.
     */
    public String ackEvents(String consumer, String cursor) {
        String name = requireConsumer(consumer);
        FeedCursor acked = FeedCursor.parse(cursor);
        List<Integer> shards = transactions.instanceShards();
        int home = transactions.home();
        Map<Integer, Long> target = new HashMap<>();
        for (Map.Entry<Integer, Long> e : acked.positions().entrySet()) {
            if (!shards.contains(e.getKey())) continue;   // a shard no longer read holds nothing to ack
            long latest = transactions.readShard(e.getKey(), Tx::latestEventSeq);
            target.put(e.getKey(), Math.max(0, Math.min(e.getValue(), latest)));
        }
        long now = System.currentTimeMillis();
        return transactions.readHome(tx -> {
            tx.advanceEventCursor(name, target.getOrDefault(home, 0L), now);
            target.forEach((shard, seq) -> {
                if (shard != home) tx.advanceEventPosition(name, shard, seq);
            });
            return stored(tx, name, shards, home).format();
        });
    }

    /**
     * {@link #ackEvents(String, String)} by a bare seq, for a log on one shard. A sharded log has no
     * single seq, so there the ack must carry the event's cursor.
     */
    public long ackEvents(String consumer, long ackedSeq) {
        List<Integer> shards = transactions.instanceShards();
        if (shards.size() > 1) {
            throw EngineException.badRequest("the event log is sharded, so a seq alone does not say where a "
                    + "consumer is: ack with the cursor of the last event handled");
        }
        int shard = shards.getFirst();
        return FeedCursor.parse(ackEvents(consumer, shard + ":" + Math.max(0, ackedSeq))).at(shard);
    }

    private static String requireConsumer(String consumer) {
        if (consumer == null || consumer.isBlank()) {
            throw EngineException.badRequest("a consumer name is required: the feed's cursor is named, "
                    + "so two consumers with no name would share one place in the log");
        }
        return consumer;
    }

    private static EventView view(Rows.Event e, int shard, String cursor) {
        return new EventView(e.seq(), e.instanceId(), e.workflow(), e.version(), e.correlationId(),
                e.type(), e.nodeId(), e.createdAt(),
                e.payload() == null ? Map.of() : Json.parseObject(e.payload()), shard, cursor);
    }

    /**
     * Drops events older than the retention window that every consumer has acknowledged (all of
     * them, while no consumer exists), shard by shard against what consumers hold on that shard.
     * Leader-only, from the housekeeper's retention sweep.
     */
    public int trimEvents(int max) {
        long cutoff = System.currentTimeMillis() - eventRetentionMillis;
        int home = transactions.home();
        int trimmed = 0;
        for (int shard : transactions.instanceShards()) {
            Long upTo = transactions.readHome(tx -> shard == home ? tx.oldestAckedSeq() : tx.oldestEventPosition(shard));
            trimmed += transactions.readShard(shard, tx -> tx.deleteEvents(cutoff, upTo, max));
        }
        int removed = trimmed;
        if (removed > 0) LOG.log(System.Logger.Level.DEBUG, () -> "trimEvents: removed " + removed + " event(s)");
        return removed;
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
        transactions.inTxVoid(instanceId, raw -> bufferedVoid(raw, tx -> {
            Instance inst = tx.lockInstance(instanceId).orElseThrow(() -> EngineException.notFound("instance"));
            Instances.requireRunning(inst);
            Token t = tx.awaitingSignal(instanceId, name)
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
        }));
    }

    /**
     * Runs {@code body} over a write-buffered view of {@code raw}, flushing before the transaction
     * commits. Every transaction that locks an instance runs this way: the lock is held until commit
     * and every other mutation of that instance waits on it, so its writes go out together at the
     * end, not one round trip each, and a token inserted and moved on is written once. A store whose
     * transactions do not roll back gets the body as it is.
     */
    private static <R> R buffered(Tx raw, java.util.function.Function<Tx, R> body) {
        if (!raw.transactional()) return body.apply(raw);
        BufferedTx tx = BufferedTx.of(raw);
        R result = body.apply(tx);
        tx.flush();
        return result;
    }

    private static void bufferedVoid(Tx raw, java.util.function.Consumer<Tx> body) {
        buffered(raw, tx -> {
            body.accept(tx);
            return null;
        });
    }

    /** A per-token action executed in its own transaction by a leader sweep. */
    private interface SweepAction {
        void apply(Tx tx, Token token);
    }

    /** Runs {@code action} once per token, each in its own transaction, isolating failures. */
    private int sweep(List<Token> due, String what, SweepAction action) {
        return sweeper.run(due, token -> what + " " + token.id, token -> {
            transactions.inTxVoid(token.id, raw -> bufferedVoid(raw, tx -> action.apply(tx, token)));
            return true;
        });
    }

    private static void logDue(String what, List<Token> due) {
        if (!due.isEmpty()) {
            LOG.log(System.Logger.Level.DEBUG, () -> what + ": " + due.size() + " due");
        }
    }

    /** Leader duty: advance sleep timers that have come due. */
    public int fireDueTimers(int max) {
        List<Token> due = transactions.readEach(tx -> tx.dueTimers(System.currentTimeMillis(), max));
        logDue("fireDueTimers", due);
        return sweep(due, "timer", this::fireTimer);
    }

    private void fireTimer(Tx tx, Token timer) {
        Instance inst = tx.lockInstance(timer.instanceId).orElse(null);
        if (!sweepable(inst)) return;
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

    /** Leader duty: retries parked for their backoff become dispatchable once it has run out. */
    public int promoteDueRetries(int max) {
        List<Token> due = transactions.readEach(tx -> tx.dueRetries(System.currentTimeMillis(), max));
        logDue("promoteDueRetries", due);
        return sweep(due, "retry promotion of", this::promoteRetry);
    }

    private void promoteRetry(Tx tx, Token parked) {
        Instance inst = tx.lockInstance(parked.instanceId).orElse(null);
        if (inst == null || !inst.status.live()) return;
        Token t = tx.findToken(parked.id).orElse(null);
        long ts = System.currentTimeMillis();
        if (t == null || t.status != TokenStatus.WAITING || t.kind == NodeKind.SLEEP || t.availableAt > ts) return;
        tokens.promote(tx, t, ts);
        LOG.log(System.Logger.Level.DEBUG, () -> "retry of " + t.id + " (attempt " + t.nextAttempt()
                + ") on instance " + inst.id + " is due");
    }

    /** Leader duty: signal waits whose deadline has passed escalate (to {@code altNext}) or fail. */
    public int fireDueSignalDeadlines(int max) {
        List<Token> due = transactions.readEach(tx -> tx.dueSignals(System.currentTimeMillis(), max));
        logDue("fireDueSignalDeadlines", due);
        return sweep(due, "signal deadline", this::escalateOrFailSignal);
    }

    private void escalateOrFailSignal(Tx tx, Token task) {
        Instance inst = tx.lockInstance(task.instanceId).orElse(null);
        if (!sweepable(inst)) return;
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
        return reclaimExpiredLeases(max, Long.MIN_VALUE);
    }

    /**
     * {@link #reclaimExpiredLeases(int)}, sparing tasks claimed before {@code spareClaimedBefore}:
     * their lease may have run out only because their worker could not reach the cell, so it stays put.
     */
    public int reclaimExpiredLeases(int max, long spareClaimedBefore) {
        List<Token> orphans = transactions.readEach(tx -> tx.expiredLeases(System.currentTimeMillis(), max)).stream()
                .filter(t -> t.startedAt == null || t.startedAt >= spareClaimedBefore)
                .toList();
        logDue("reclaimExpiredLeases", orphans);
        return sweep(orphans, "reclaim of", this::reclaimOrphan);
    }

    private void reclaimOrphan(Tx tx, Token orphan) {
        Instance inst = tx.lockInstance(orphan.instanceId).orElse(null);
        if (inst == null) return;
        Token t = tx.findToken(orphan.id).orElse(null);
        if (t == null || !t.hasExpiredLeaseAt(System.currentTimeMillis())) return;
        Node node = definitions.graph(tx, t.workflow, t.version).node(t.nodeId);
        long ts = System.currentTimeMillis();
        LOG.log(System.Logger.Level.DEBUG, () -> "reclaim: " + node.name() + " of instance " + inst.id
                + " orphaned by worker " + t.leaseOwner);
        settleFailure(tx, inst, t, node, "lease expired (worker unreachable)", "lease expired", true, ts);
    }

    /** Fails a task. Retries per the node's policy; when exhausted the whole instance fails. */
    public void fail(String taskId, String leaseOwner, String message, boolean retryable) {
        fail(taskId, leaseOwner, message, retryable, null);
    }

    /** The same, for a caller that serves only the queues {@code queues} accepts (null: every queue). */
    public void fail(String taskId, String leaseOwner, String message, boolean retryable, Predicate<String> queues) {
        transactions.inTxVoid(taskId, raw -> bufferedVoid(raw, tx -> {
            LockedTask locked = Tokens.lock(tx, taskId);
            Instance inst = locked.inst();
            Token t = locked.token();
            Tokens.requireQueue(t, queues);
            Tokens.requireLease(t, leaseOwner);
            if (!inst.status.live()) return;
            Node node = definitions.graph(tx, t.workflow, t.version).node(t.nodeId);
            if (StepIo.ENABLED) StepIo.record(t, Scopes.dispatchContext(inst, t), null);
            settleFailure(tx, inst, t, node, message, message, retryable, System.currentTimeMillis());
        }));
    }

    /**
     * Hands a token failure to {@link Tokens#reportFailure} and applies what
     * the verdict means
     * for the instance: nothing while retries remain, the comp-log for an exhausted compensator, and
     * otherwise the instance itself (as {@code node.name() + ": " + failReason}).
     */
    private void settleFailure(Tx tx, Instance inst, Token t, Node node,
                               String lastError, String failReason, boolean retryable, long now) {
        Tokens.Outcome outcome = Tokens.reportFailure(tx, t, node, lastError, failReason, retryable,
                retryParkFromMillis, now);
        if (!(outcome instanceof Tokens.Outcome.Exhausted(String reason, Long compSeq))) return;
        if (compSeq != null) {
            instances.compensatorExhausted(tx, inst, node, compSeq, reason, now);
            return;
        }
        if (inst.status.running()) {
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
        int purged = transactions.sumEach(tx -> Instances.purgeTerminalBefore(tx, cutoff, max));
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
