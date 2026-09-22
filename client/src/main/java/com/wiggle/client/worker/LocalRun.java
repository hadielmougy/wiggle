package com.wiggle.client.worker;

import com.wiggle.client.WiggleClient;
import com.wiggle.core.AdvanceResult;
import com.wiggle.core.ExecutionMode;
import com.wiggle.core.GraphTraversal;
import com.wiggle.core.Node;
import com.wiggle.core.NodeKind;
import com.wiggle.core.TaskActivation;
import com.wiggle.core.WorkflowDefinition;

import java.util.ArrayList;
import java.util.List;

/**
 * One local execution run (LOCAL_SYNC / LOCAL_ASYNC): consecutive same-queue steps executed
 * in-worker until the next node is a boundary (sleep / fork / join / user task / other queue /
 * end) or the instance stops running. LOCAL_SYNC flushes every step (one-step crash blast
 * radius); LOCAL_ASYNC buffers up to {@code localBatchSize} steps and flushes the run in one
 * call. Owning the chain state here keeps each step's logic flat.
 */
final class LocalRun {

    private static final System.Logger LOG = System.getLogger(LocalRun.class.getName());

    private final Worker w;
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

    LocalRun(Worker w, TaskActivation task, WorkflowDefinition def) {
        this.w = w;
        this.def = def;
        this.leaseOwner = task.leaseOwner();
        this.instanceId = task.instanceId();
        this.maxBatch = task.executionMode() == ExecutionMode.LOCAL_ASYNC ? w.options().localBatchSize() : 1;
        this.serverTaskId = task.taskId();
        this.node = def.node(task.nodeId());
        this.ctx = task.context();
        this.baseContext = task.baseContext();
        this.itemIndex = task.itemIndex();
        this.itemMapKey = task.itemMapKey();
        this.attempt = task.attempt();   // 1-based; continuation tokens are fresh (attempt 1)
    }

    void run() {
        Heartbeat lease = new Heartbeat(w.heartbeatPool(),
                extend -> w.client().heartbeat(serverTaskId, leaseOwner, extend),
                w.options().lease().toMillis(), serverTaskId);
        lease.start();
        try {
            boolean chaining = true;
            // Re-checked between steps (never mid-handler), so a shutdown drains promptly:
            // the step already in flight finishes normally, then the loop stops here instead
            // of picking up another one.
            while (chaining && w.isRunning()) {
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
            w.client().advanceRun(serverTaskId, leaseOwner, List.copyOf(buffer), true);
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
        ActivityHandler handler = w.registrations().handlerFor(node.activity());
        if (handler == null) {
            failRun("no handler registered for activity '" + node.activity() + "'", false);
            return false;
        }
        Invocation outcome = invoke(handler);
        if (!outcome.ok()) return false;
        if (node.kind() == NodeKind.PREDICATE && !(outcome.result() instanceof Boolean)) {
            failRun("predicate '" + node.name() + "' returned " + Worker.typeName(outcome.result()), false);
            return false;
        }
        return advance(outcome.result(), outcome.startedAt(), outcome.finishedAt());
    }

    private Invocation invoke(ActivityHandler handler) {
        Step.begin(new Step.Info(attempt, node.name(), instanceId,
                baseContext, baseContext != null, itemIndex, itemMapKey));
        long startedAt = System.currentTimeMillis();
        long t0 = System.nanoTime();
        try {
            Object result = handler.invoke(ctx);
            return Invocation.ok(result, startedAt, startedAt + (System.nanoTime() - t0) / 1_000_000);
        } catch (PermanentActivityException e) {
            failRun(Worker.describe(e), false);
            return Invocation.failed();
        } catch (Exception e) {
            String stepName = node.name();
            LOG.log(System.Logger.Level.DEBUG,
                    () -> "local step " + stepName + " of " + instanceId + " failed: " + e);
            failRun(Worker.describe(e), true);
            return Invocation.failed();
        } finally {
            Step.end();
        }
    }

    /** Records the step, flushes when due, and moves to the successor; false = run is over. */
    private boolean advance(Object result, long startedAt, long finishedAt) {
        boolean isPredicate = node.kind() == NodeKind.PREDICATE;
        boolean predicateValue = isPredicate && (Boolean) result;
        Node next = def.node(GraphTraversal.successor(node, predicateValue));
        boolean handback = GraphTraversal.classify(next, w.servedQueues()) != null;
        buffer.add(isPredicate
                ? new WiggleClient.StepReport(node.id(), null, predicateValue, startedAt, finishedAt)
                : new WiggleClient.StepReport(node.id(), result, null, startedAt, finishedAt));
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
        // A compensable step flushes like a checkpoint: its input/result snapshots must be
        // durably captured (the engine's comp-log) before anything later can fail.
        return handback || def.checkpoints().contains(node.id()) || node.compensable()
                || buffer.size() >= maxBatch;
    }

    /** Flushes the buffer; true = the server leased us the continuation, keep chaining. */
    private boolean flushAndContinue(boolean handback) {
        // A final handback needs nothing back but durability, so LOCAL_ASYNC routes it through
        // the worker's batcher -- concurrent runs land in one AdvanceMany call. A mid-chain flush
        // needs the leased continuation id synchronously and stays a single call.
        AdvanceResult advanced = handback && maxBatch > 1 && w.options().crossInstanceBatching()
                ? w.handbacks().handback(instanceId, serverTaskId, leaseOwner, List.copyOf(buffer))
                : w.client().advanceRun(serverTaskId, leaseOwner, List.copyOf(buffer), handback);
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
        w.client().fail(serverTaskId, leaseOwner, message, retryable);
    }

    /** @return true if the instance is still running (safe to report a failure) */
    private boolean flushBeforeFailure() {
        if (buffer.isEmpty()) return true;
        AdvanceResult advanced = w.client().advanceRun(serverTaskId, leaseOwner, List.copyOf(buffer), false);
        buffer.clear();
        if (advanced.nextTaskId() != null) serverTaskId = advanced.nextTaskId();
        return advanced.running();
    }

    /** The outcome of invoking a handler: a result, or "already reported as failed". */
    private record Invocation(boolean ok, Object result, long startedAt, long finishedAt) {
        static Invocation ok(Object result, long startedAt, long finishedAt) { return new Invocation(true, result, startedAt, finishedAt); }
        static Invocation failed() { return new Invocation(false, null, 0, 0); }
    }

    /** Mirrors the server: a step's return REPLACES the context (null = unchanged, no merge). */
    private static Object applyReplace(Object ctx, Object result) {
        return result == null ? ctx : result;
    }
}
