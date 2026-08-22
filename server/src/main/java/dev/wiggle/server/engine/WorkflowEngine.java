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

    public String start(String workflow, Integer version, Object context, String correlationId) {
        return storage.inTx(tx -> {
            int v = version != null ? version : tx.latestVersion(workflow).orElseThrow(
                    () -> EngineException.notFound("workflow '" + workflow + "'"));
            LazyGraph def = definitions.graph(tx, workflow, v);
            long now = System.currentTimeMillis();

            Instance inst = new Instance();
            inst.id = Ids.next("wfi");
            inst.workflow = def.name();
            inst.version = def.version();
            inst.correlationId = correlationId;
            inst.status = InstanceStatus.RUNNING;
            inst.contextJson = Json.write(context == null ? Map.of() : context);
            inst.createdAt = now;
            inst.updatedAt = now;
            tx.insertInstance(inst);

            Token t = newToken(inst, def.startNode(), "", now);
            tx.insertToken(t);

            LOG.log(System.Logger.Level.DEBUG, () -> "start: instance " + inst.id + " of " + def.key()
                    + " at node " + def.startNode() + " correlationId=" + correlationId);
            drive(tx, def, inst, new ArrayDeque<>(List.of(t)), now);
            return inst.id;
        });
    }

    public void cancel(String instanceId, String reason) {
        storage.inTxVoid(tx -> {
            Instance inst = tx.lockInstance(instanceId).orElseThrow(() -> EngineException.notFound("instance"));
            if (inst.status != InstanceStatus.RUNNING) {
                LOG.log(System.Logger.Level.DEBUG, () ->
                        "cancel: instance " + instanceId + " ignored, already " + inst.status);
                return;
            }
            long now = System.currentTimeMillis();
            cancelActiveTokens(tx, inst.id, null, now);
            inst.status = InstanceStatus.CANCELLED;
            inst.terminationReason = reason;
            inst.updatedAt = now;
            tx.updateInstance(inst);
            LOG.log(System.Logger.Level.DEBUG, () -> "cancel: instance " + instanceId + " cancelled, reason=" + reason);
        });
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

    /** Leases up to {@code max} tasks for a worker. Returns immediately; long-polling lives in the HTTP layer. */
    public List<TaskActivation> poll(String workerId, Set<String> queues, int max, Long leaseMillis) {
        long now = System.currentTimeMillis();
        long lease = leaseMillis == null || leaseMillis <= 0 ? defaultLeaseMillis : leaseMillis;
        long until = now + lease;
        List<TaskActivation> out = storage.inTx(tx -> {
            List<Token> claimed = tx.claimTasks(workerId, queues, max, now, until);
            List<TaskActivation> activations = new ArrayList<>(claimed.size());
            for (Token t : claimed) {
                Instance inst = tx.findInstance(t.instanceId).orElse(null);
                if (inst == null || inst.status != InstanceStatus.RUNNING) continue;
                LazyGraph def = definitions.graph(tx, t.workflow, t.version);
                Node node = def.node(t.nodeId);
                ExecutionMode mode = resolveMode(definitions.executionMode(tx, t.workflow, t.version));
                activations.add(new TaskActivation(t.id, inst.id, inst.workflow, inst.version, node.id(), node.name(),
                        node.activity(), node.kind(), t.attempt + 1, until, workerId, Json.parse(inst.contextJson), mode));
            }
            return activations;
        });
        if (!out.isEmpty()) {
            LOG.log(System.Logger.Level.DEBUG, () -> "poll: worker " + workerId + " queues=" + queues
                    + " claimed " + out.size() + " task(s): "
                    + out.stream().map(a -> a.taskId() + "@" + a.stepName()).toList());
        }
        return out;
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

    /**
     * Completes a task. For TASK nodes {@code result} is shallow-merged into the instance
     * context; for PREDICATE nodes it must carry a boolean under {@code "value"}.
     */
    public void complete(String taskId, String leaseOwner, Object result) {
        storage.inTxVoid(tx -> {
            Token probe = tx.findToken(taskId).orElseThrow(() -> EngineException.notFound("task"));
            // Take the instance lock first, then re-read the token under it.
            Instance inst = tx.lockInstance(probe.instanceId).orElseThrow(() -> EngineException.notFound("instance"));
            Token t = tx.findToken(taskId).orElseThrow(() -> EngineException.notFound("task"));
            requireLease(t, leaseOwner);
            if (inst.status != InstanceStatus.RUNNING) {
                throw EngineException.conflict("instance " + inst.id + " is " + inst.status);
            }

            long now = System.currentTimeMillis();
            LazyGraph def = definitions.graph(tx, t.workflow, t.version);
            Node node = def.node(t.nodeId);

            String next;
            if (node.kind() == NodeKind.PREDICATE) {
                boolean value = predicateValue(result);
                next = value ? node.next() : node.altNext();
                LOG.log(System.Logger.Level.DEBUG, () -> "complete: predicate " + node.name()
                        + " of instance " + inst.id + " evaluated " + value + " -> " + next);
            } else {
                mergeContext(inst, result);
                next = node.next();
                LOG.log(System.Logger.Level.DEBUG, () -> "complete: task " + node.name()
                        + " of instance " + inst.id + " done -> " + next);
            }

            t.status = TokenStatus.DONE;
            t.leaseOwner = null;
            t.leaseExpiresAt = 0;
            t.updatedAt = now;
            tx.updateToken(t);

            inst.updatedAt = now;
            tx.updateInstance(inst);

            Token cont = newToken(inst, next, t.joinStack, now);
            tx.insertToken(cont);
            drive(tx, def, inst, new ArrayDeque<>(List.of(cont)), now);
        });
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
            Token probe = tx.findToken(startTaskId).orElseThrow(() -> EngineException.notFound("task"));
            Instance inst = tx.lockInstance(probe.instanceId).orElseThrow(() -> EngineException.notFound("instance"));
            long now = System.currentTimeMillis();
            long lease = now + defaultLeaseMillis;
            if (inst.status != InstanceStatus.RUNNING) {
                return new AdvanceOutcome(inst.status.name(), 0, null);
            }
            LazyGraph def = definitions.graph(tx, inst.workflow, inst.version);
            Token current = tx.findToken(startTaskId).orElseThrow(() -> EngineException.notFound("task"));
            requireLease(current, leaseOwner);

            String nextTaskId = null;
            for (int i = 0; i < steps.size(); i++) {
                StepInput step = steps.get(i);
                Node node = def.node(current.nodeId);
                if (!node.id().equals(step.nodeId())) {
                    throw EngineException.conflict("reported step " + step.nodeId() + " but token "
                            + current.id + " is at " + node.id());
                }
                boolean value = step.predicateValue() != null && step.predicateValue();
                String next;
                if (node.kind() == NodeKind.PREDICATE) {
                    next = GraphTraversal.successor(node, value);
                } else {
                    mergeContext(inst, step.merge());
                    next = node.next();
                }
                current.status = TokenStatus.DONE;
                current.leaseOwner = null;
                current.leaseExpiresAt = 0;
                current.updatedAt = now;
                tx.updateToken(current);
                inst.updatedAt = now;
                tx.updateInstance(inst);

                Token cont = newToken(inst, next, current.joinStack, now);
                Node nextNode = def.node(next);
                boolean lastStep = i == steps.size() - 1;
                boolean nextRunnable = nextNode.kind() == NodeKind.TASK || nextNode.kind() == NodeKind.PREDICATE;

                if ((lastStep && finalHandback) || !nextRunnable) {
                    // Hand back: drive the continuation normally (READY for a worker, or a boundary).
                    tx.insertToken(cont);
                    LOG.log(System.Logger.Level.DEBUG, () -> "advanceRun: instance " + inst.id
                            + " handing back at " + next + " (" + nextNode.kind() + ")");
                    drive(tx, def, inst, new ArrayDeque<>(List.of(cont)), now);
                    nextTaskId = null;
                    break;
                }
                // Keep the chain on this worker: lease the continuation straight back, no poll.
                cont.status = TokenStatus.RUNNING;
                cont.kind = nextNode.kind();
                cont.activity = nextNode.activity();
                cont.queue = nextNode.queue();
                cont.leaseOwner = leaseOwner;
                cont.leaseExpiresAt = lease;
                cont.availableAt = now;
                cont.updatedAt = now;
                tx.insertToken(cont);
                current = cont;
                nextTaskId = cont.id;
                LOG.log(System.Logger.Level.DEBUG, () -> "advanceRun: instance " + inst.id
                        + " chaining locally " + node.name() + " -> " + next);
            }
            return new AdvanceOutcome(inst.status.name(), lease, nextTaskId);
        });
    }

    /** The user tasks currently awaiting an external completion, oldest first. */
    public List<Token> pendingUserTasks(int max) {
        return storage.inTx(tx -> tx.pendingUserTasks(max));
    }

    /**
     * Completes a user task on behalf of an external actor. Unlike {@link #complete}, there is
     * no lease to hold -- the token only has to be AWAITING. {@code result} is merged into the
     * instance context and the flow advances down the task's completion path.
     */
    public void completeUserTask(String taskId, Object result) {
        storage.inTxVoid(tx -> {
            Token probe = tx.findToken(taskId).orElseThrow(() -> EngineException.notFound("task"));
            Instance inst = tx.lockInstance(probe.instanceId).orElseThrow(() -> EngineException.notFound("instance"));
            Token t = tx.findToken(taskId).orElseThrow(() -> EngineException.notFound("task"));
            if (t.status != TokenStatus.AWAITING || t.kind != NodeKind.USER_TASK) {
                throw EngineException.conflict("task " + t.id + " is " + t.status + ", not an awaiting user task");
            }
            if (inst.status != InstanceStatus.RUNNING) {
                throw EngineException.conflict("instance " + inst.id + " is " + inst.status);
            }
            long now = System.currentTimeMillis();
            LazyGraph def = definitions.graph(tx, t.workflow, t.version);
            Node node = def.node(t.nodeId);

            mergeContext(inst, result);
            t.status = TokenStatus.DONE;
            t.updatedAt = now;
            tx.updateToken(t);
            inst.updatedAt = now;
            tx.updateInstance(inst);

            Token cont = newToken(inst, node.next(), t.joinStack, now);
            tx.insertToken(cont);
            LOG.log(System.Logger.Level.DEBUG, () -> "completeUserTask: user task " + node.name()
                    + " of instance " + inst.id + " completed externally -> " + node.next());
            drive(tx, def, inst, new ArrayDeque<>(List.of(cont)), now);
        });
    }

    /** Leader duty: user tasks whose deadline has passed escalate (to {@code altNext}) or fail. */
    public int fireDueUserTaskDeadlines(int max) {
        long now = System.currentTimeMillis();
        List<Token> due = storage.inTx(tx -> tx.dueUserTasks(now, max));
        if (!due.isEmpty()) {
            LOG.log(System.Logger.Level.DEBUG, () -> "fireDueUserTaskDeadlines: " + due.size() + " due");
        }
        int fired = 0;
        for (Token task : due) {
            try {
                storage.inTxVoid(tx -> {
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
                    if (node.altNext() != null) {
                        Token cont = newToken(inst, node.altNext(), t.joinStack, ts);
                        tx.insertToken(cont);
                        LOG.log(System.Logger.Level.DEBUG, () -> "user task " + node.name()
                                + " of instance " + inst.id + " missed its deadline -> escalating to " + node.altNext());
                        drive(tx, def, inst, new ArrayDeque<>(List.of(cont)), ts);
                    } else {
                        LOG.log(System.Logger.Level.DEBUG, () -> "user task " + node.name()
                                + " of instance " + inst.id + " missed its deadline, no escalation -> failing instance");
                        failInstance(tx, inst, "user task '" + node.name() + "' timed out", ts);
                    }
                });
                fired++;
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.WARNING, "user-task deadline " + task.id + " failed: " + e);
            }
        }
        return fired;
    }

    /** Fails a task. Retries per the node's policy; when exhausted the whole instance fails. */
    public void fail(String taskId, String leaseOwner, String message, boolean retryable) {
        storage.inTxVoid(tx -> {
            Token probe = tx.findToken(taskId).orElseThrow(() -> EngineException.notFound("task"));
            Instance inst = tx.lockInstance(probe.instanceId).orElseThrow(() -> EngineException.notFound("instance"));
            Token t = tx.findToken(taskId).orElseThrow(() -> EngineException.notFound("task"));
            requireLease(t, leaseOwner);
            if (inst.status != InstanceStatus.RUNNING) return;

            long now = System.currentTimeMillis();
            LazyGraph def = definitions.graph(tx, t.workflow, t.version);
            Node node = def.node(t.nodeId);
            RetryPolicy policy = node.retry() == null ? RetryPolicy.forever() : node.retry();

            t.attempt++;
            t.lastError = message;
            t.leaseOwner = null;
            t.leaseExpiresAt = 0;
            t.updatedAt = now;

            if (retryable && t.attempt < policy.maxAttempts()) {
                t.status = TokenStatus.READY;
                t.availableAt = now + policy.backoffMillis(t.attempt);
                tx.updateToken(t);
                long backoffMs = t.availableAt - now;
                LOG.log(System.Logger.Level.DEBUG, () -> "fail: " + node.name() + " of instance " + inst.id
                        + " failed (" + message + "), retrying attempt " + t.attempt
                        + "/" + policy.maxAttempts() + " in " + backoffMs + "ms");
                return;
            }

            t.status = TokenStatus.FAILED;
            tx.updateToken(t);
            LOG.log(System.Logger.Level.DEBUG, () -> "fail: " + node.name() + " of instance " + inst.id
                    + " exhausted retries (attempt " + t.attempt + "/" + policy.maxAttempts()
                    + ", retryable=" + retryable + ") -> failing instance");
            failInstance(tx, inst, node.name() + ": " + message, now);
        });
    }

    private static boolean predicateValue(Object result) {
        if (result instanceof Boolean b) return b;
        if (result instanceof Map<?, ?> m) {
            Object v = m.get("value");
            if (v instanceof Boolean b) return b;
        }
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

    /** Leader duty: advance sleep timers that have come due. */
    public int fireDueTimers(int max) {
        long now = System.currentTimeMillis();
        List<Token> due = storage.inTx(tx -> tx.dueTimers(now, max));
        if (!due.isEmpty()) {
            LOG.log(System.Logger.Level.DEBUG, () -> "fireDueTimers: " + due.size() + " due");
        }
        int fired = 0;
        for (Token timer : due) {
            try {
                storage.inTxVoid(tx -> {
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
                    Token cont = newToken(inst, node.next(), t.joinStack, ts);
                    tx.insertToken(cont);
                    LOG.log(System.Logger.Level.DEBUG, () -> "timer " + node.name()
                            + " of instance " + inst.id + " fired -> " + node.next());
                    drive(tx, def, inst, new ArrayDeque<>(List.of(cont)), ts);
                });
                fired++;
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.WARNING, "timer " + timer.id + " failed: " + e);
            }
        }
        return fired;
    }

    /** Leader duty: return tasks whose worker died back to the ready pool. */
    public int reclaimExpiredLeases(int max) {
        long now = System.currentTimeMillis();
        List<Token> orphans = storage.inTx(tx -> tx.expiredLeases(now, max));
        if (!orphans.isEmpty()) {
            LOG.log(System.Logger.Level.DEBUG, () -> "reclaimExpiredLeases: " + orphans.size() + " expired");
        }
        int reclaimed = 0;
        for (Token orphan : orphans) {
            try {
                storage.inTxVoid(tx -> {
                    Instance inst = tx.lockInstance(orphan.instanceId).orElse(null);
                    if (inst == null) return;
                    Token t = tx.findToken(orphan.id).orElse(null);
                    if (t == null || t.status != TokenStatus.RUNNING || t.leaseExpiresAt >= System.currentTimeMillis()) return;
                    LazyGraph def = definitions.graph(tx, t.workflow, t.version);
                    Node node = def.node(t.nodeId);
                    RetryPolicy policy = node.retry() == null ? RetryPolicy.forever() : node.retry();
                    long ts = System.currentTimeMillis();
                    String previousOwner = t.leaseOwner;
                    t.attempt++;
                    t.leaseOwner = null;
                    t.leaseExpiresAt = 0;
                    t.lastError = "lease expired (worker unreachable)";
                    t.updatedAt = ts;
                    if (t.attempt < policy.maxAttempts()) {
                        t.status = TokenStatus.READY;
                        t.availableAt = ts + policy.backoffMillis(t.attempt);
                        tx.updateToken(t);
                        LOG.log(System.Logger.Level.DEBUG, () -> "reclaim: " + node.name() + " of instance " + inst.id
                                + " orphaned by worker " + previousOwner + ", redispatchable (attempt " + t.attempt + ")");
                    } else {
                        t.status = TokenStatus.FAILED;
                        tx.updateToken(t);
                        LOG.log(System.Logger.Level.DEBUG, () -> "reclaim: " + node.name() + " of instance " + inst.id
                                + " orphaned by worker " + previousOwner + ", retries exhausted -> failing instance");
                        if (inst.status == InstanceStatus.RUNNING) {
                            failInstance(tx, inst, node.name() + ": lease expired", ts);
                        }
                    }
                });
                reclaimed++;
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.WARNING, "reclaim of " + orphan.id + " failed: " + e);
            }
        }
        return reclaimed;
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

    /**
     * Advances tokens until each one is parked on something that needs the outside
     * world: a worker (READY), a clock (WAITING), a sibling (JOINED), or nothing at
     * all (DONE at an END node).
     */
    private void drive(Tx tx, LazyGraph def, Instance inst, Deque<Token> work, long now) {
        int guard = 0;
        while (!work.isEmpty()) {
            if (++guard > 10_000) throw new IllegalStateException("cycle detected in workflow " + def.key());
            Token t = work.pop();
            Node node = def.node(t.nodeId);
            TokenStatus before = t.status;
            switch (node.kind()) {
                case TASK, PREDICATE -> {
                    t.status = TokenStatus.READY;
                    t.kind = node.kind();
                    t.activity = node.activity();
                    t.queue = node.queue();
                    t.availableAt = now;
                    t.updatedAt = now;
                    tx.updateToken(t);
                    LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                            + node.name() + " (" + node.kind() + ") " + before + " -> READY, queue=" + node.queue());
                }
                case SLEEP -> {
                    t.status = TokenStatus.WAITING;
                    t.kind = NodeKind.SLEEP;
                    t.availableAt = now + node.sleepMillis();
                    t.updatedAt = now;
                    tx.updateToken(t);
                    LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                            + node.name() + " (SLEEP) " + before + " -> WAITING until " + t.availableAt
                            + " (" + node.sleepMillis() + "ms)");
                }
                case USER_TASK -> {
                    // Park until an external actor completes it. No worker leases an AWAITING
                    // token; a positive availableAt is the (optional) deadline the leader sweeps.
                    t.status = TokenStatus.AWAITING;
                    t.kind = NodeKind.USER_TASK;
                    t.activity = node.name();     // surfaced to the task list as the human name
                    t.availableAt = node.sleepMillis() > 0 ? now + node.sleepMillis() : 0;
                    t.updatedAt = now;
                    tx.updateToken(t);
                    LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                            + node.name() + " (USER_TASK) " + before + " -> AWAITING, deadline="
                            + (t.availableAt > 0 ? t.availableAt : "none"));
                }
                case FORK -> {
                    t.status = TokenStatus.DONE;
                    t.kind = NodeKind.FORK;
                    t.updatedAt = now;
                    tx.updateToken(t);
                    String group = Ids.next("jg");
                    String childStack = t.pushJoinStack(group);
                    for (String branchStart : node.branches()) {
                        Token child = newToken(inst, branchStart, childStack, now);
                        tx.insertToken(child);
                        work.push(child);
                    }
                    LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                            + node.name() + " (FORK) " + before + " -> DONE, spawned " + node.branches().size()
                            + " branch(es) in group " + group + ": " + node.branches());
                }
                case JOIN -> {
                    String group = t.currentJoinGroup();
                    t.status = TokenStatus.JOINED;
                    t.kind = NodeKind.JOIN;
                    t.updatedAt = now;
                    tx.updateToken(t);
                    List<Token> atBarrier = tx.tokensOf(inst.id).stream()
                            .filter(x -> x.status == TokenStatus.JOINED)
                            .filter(x -> node.id().equals(x.nodeId))
                            .filter(x -> Objects.equals(group, x.currentJoinGroup()))
                            .toList();
                    if (atBarrier.size() >= node.expected()) {
                        // Consume the barrier: these tokens have served their purpose, and
                        // leaving them parked would keep the instance looking active forever.
                        for (Token parked : atBarrier) {
                            parked.status = TokenStatus.DONE;
                            parked.updatedAt = now;
                            tx.updateToken(parked);
                        }
                        Token cont = newToken(inst, node.next(), t.popJoinStack(), now);
                        tx.insertToken(cont);
                        work.push(cont);
                        LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                                + node.name() + " (JOIN) " + before + " -> JOINED, barrier " + group
                                + " satisfied (" + atBarrier.size() + "/" + node.expected() + ") -> " + node.next());
                    } else {
                        int atBarrierSize = atBarrier.size();
                        LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                                + node.name() + " (JOIN) " + before + " -> JOINED, waiting on barrier " + group
                                + " (" + atBarrierSize + "/" + node.expected() + ")");
                    }
                }
                case END -> {
                    t.status = TokenStatus.DONE;
                    t.kind = NodeKind.END;
                    t.updatedAt = now;
                    tx.updateToken(t);
                    if (!node.success()) {
                        LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                                + node.name() + " (END) " + before + " -> DONE, unsuccessful end -> failing instance");
                        failInstance(tx, inst, node.reason() == null ? "terminated" : node.reason(), now);
                        return;
                    }
                    boolean anyActive = tx.tokensOf(inst.id).stream().anyMatch(Token::isActive);
                    if (!anyActive && work.isEmpty()) {
                        inst.status = InstanceStatus.COMPLETED;
                        inst.terminationReason = node.reason();
                        inst.updatedAt = now;
                        tx.updateInstance(inst);
                        LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                                + node.name() + " (END) " + before + " -> DONE, no tokens remain -> instance COMPLETED"
                                + (node.reason() != null ? " (" + node.reason() + ")" : ""));
                    } else {
                        LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                                + node.name() + " (END) " + before + " -> DONE, other tokens still active");
                    }
                }
            }
        }
    }

    private void failInstance(Tx tx, Instance inst, String error, long now) {
        cancelActiveTokens(tx, inst.id, null, now);
        inst.status = InstanceStatus.FAILED;
        inst.error = error;
        inst.updatedAt = now;
        tx.updateInstance(inst);
        LOG.log(System.Logger.Level.INFO, () -> "instance " + inst.id + " failed: " + error);
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

    private static Token newToken(Instance inst, String nodeId, String joinStack, long now) {
        Token t = new Token();
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
