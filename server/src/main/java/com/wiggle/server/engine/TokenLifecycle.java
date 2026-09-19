package com.wiggle.server.engine;

import com.wiggle.core.Doc;
import com.wiggle.core.ExecutionMode;
import com.wiggle.core.Ids;
import com.wiggle.core.Node;
import com.wiggle.core.NodeKind;
import com.wiggle.core.RetryPolicy;
import com.wiggle.core.TaskActivation;
import com.wiggle.core.WorkflowVersion;
import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.Rows.InstanceStatus;
import com.wiggle.server.store.Rows.Token;
import com.wiggle.server.store.Rows.TokenStatus;
import com.wiggle.server.store.TokenPayload;
import com.wiggle.server.store.Tx;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * The token's state machine: every write to a token's status, lease, attempt and availability
 * happens here, and nowhere else in the engine.
 *
 * <p>A token is minted at a node, parked on whatever the node needs from the outside world
 * (a worker, a clock, an external actor, a sibling), claimed under a lease, and settled. It may
 * read an instance's status as a dispatch guard, but it never assigns one: a token transition
 * whose meaning reaches the instance reports an {@link Outcome} for the caller to act on. That
 * is what keeps this the lower of the two lifecycles -- {@link InstanceLifecycle} depends on it,
 * never the reverse.
 */
final class TokenLifecycle {

    private static final System.Logger LOG = System.getLogger(TokenLifecycle.class.getName());

    private final DefinitionRegistry definitions;
    private final Transactions transactions;

    TokenLifecycle(DefinitionRegistry definitions, Transactions transactions) {
        this.definitions = definitions;
        this.transactions = transactions;
    }

    /** A fresh READY token for {@code inst} at {@code nodeId}. Not yet inserted. */
    static Token mint(Instance inst, String nodeId, String joinStack, TokenPayload payload, long now) {
        Token t = new Token();
        t.payload = payload == null ? TokenPayload.EMPTY : payload;
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

    /** Marks a token consumed and releases its lease. */
    static void settle(Tx tx, Token t, long now) {
        t.status = TokenStatus.DONE;
        t.leaseOwner = null;
        t.leaseExpiresAt = 0;
        t.updatedAt = now;
        tx.updateToken(t);
    }

    /** Parks a token for a worker to claim, and marks its queue for the post-commit wake. */
    void parkReady(Tx tx, Instance inst, Token t, Node node, long now) {
        TokenStatus before = t.status;
        t.status = TokenStatus.READY;
        t.kind = node.kind();
        t.activity = node.activity();
        t.queue = node.queue();
        t.availableAt = now;
        t.updatedAt = now;
        tx.updateToken(t);
        transactions.wake(node.queue());
        LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                + node.name() + " (" + node.kind() + ") " + before + " -> READY, queue=" + node.queue());
    }

    /** Marks a queue for the post-commit wake, for a token inserted READY rather than parked
     *  there -- the saga reverse pass mints its undo tasks that way. */
    void wake(String queue) {
        transactions.wake(queue);
    }

    /** Parks a token on the clock until {@code availableAt}. */
    static TokenStatus parkWaiting(Tx tx, Token t, NodeKind kind, long availableAt, long now) {
        TokenStatus before = t.status;
        t.status = TokenStatus.WAITING;
        t.kind = kind;
        t.availableAt = availableAt;
        t.updatedAt = now;
        tx.updateToken(t);
        return before;
    }

    /** Parks a token on an external actor: a signal, or a child instance. A positive
     *  {@code deadline} is the (optional) time the leader sweeps it at. */
    static TokenStatus parkAwaiting(Tx tx, Token t, NodeKind kind, String activity, long deadline, long now) {
        TokenStatus before = t.status;
        t.status = TokenStatus.AWAITING;
        t.kind = kind;
        t.activity = activity;
        t.availableAt = deadline;
        t.updatedAt = now;
        tx.updateToken(t);
        return before;
    }

    /** Parks a token at a join barrier, waiting on its siblings. */
    static TokenStatus parkJoined(Tx tx, Token t, long now) {
        TokenStatus before = t.status;
        t.status = TokenStatus.JOINED;
        t.kind = NodeKind.JOIN;
        t.updatedAt = now;
        tx.updateToken(t);
        return before;
    }

    /** Spends a token at a node that consumes it outright -- a fork that has spawned its branches,
     *  or an END that has run out of flow. Returns the status it held, for the caller's log line. */
    static TokenStatus spend(Tx tx, Token t, NodeKind kind, long now) {
        TokenStatus before = t.status;
        t.status = TokenStatus.DONE;
        t.kind = kind;
        t.updatedAt = now;
        tx.updateToken(t);
        return before;
    }

    /** Consumes a satisfied barrier: these tokens have served their purpose, and leaving them
     *  parked would keep the instance looking active forever. */
    static void consumeBarrier(Tx tx, List<Token> atBarrier, long now) {
        for (Token parked : atBarrier) {
            parked.status = TokenStatus.DONE;
            parked.updatedAt = now;
            tx.updateToken(parked);
        }
    }

    /** Keeps a chain on the worker that reported it: leases the continuation straight back,
     *  never exposing it to {@link Dispatch#poll}. */
    static void leaseBack(Tx tx, Token cont, Node nextNode, String leaseOwner, long lease, long now) {
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

    /** Fails a parked token outright, with no retry: the thing it was waiting on can no longer
     *  arrive. A sub-workflow that ended badly leaves its parent's token this way. */
    static void failParked(Tx tx, Token t, long now) {
        t.status = TokenStatus.FAILED;
        t.updatedAt = now;
        tx.updateToken(t);
    }

    /** Cancels every token of an instance that is still active. */
    static void cancelAll(Tx tx, String instanceId, long now) {
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

    /** Whether any token of the instance is still active -- what decides an END node completes it. */
    static boolean anyActive(Tx tx, String instanceId) {
        return tx.tokensOf(instanceId).stream().anyMatch(Token::isActive);
    }

    /** A task's token re-read under its instance's write lock. */
    record LockedTask(Instance inst, Token token) {}

    /** Takes the instance lock first, then re-reads the token under it. */
    static LockedTask lock(Tx tx, String taskId) {
        Token probe = tx.findToken(taskId).orElseThrow(() -> EngineException.notFound("task"));
        Instance inst = tx.lockInstance(probe.instanceId).orElseThrow(() -> EngineException.notFound("instance"));
        Token token = tx.findToken(taskId).orElseThrow(() -> EngineException.notFound("task"));
        return new LockedTask(inst, token);
    }

    static void requireLease(Token t, String leaseOwner) {
        if (t.status != TokenStatus.RUNNING) {
            throw EngineException.conflict("task " + t.id + " is " + t.status + ", not RUNNING");
        }
        if (leaseOwner != null && !leaseOwner.equals(t.leaseOwner)) {
            throw EngineException.conflict("lease for task " + t.id + " is held by " + t.leaseOwner);
        }
    }

    /** Extends the lease of an in-flight task (worker heartbeat for long-running steps). */
    static long extendLease(Tx tx, String taskId, String leaseOwner, long extraMillis) {
        Token t = tx.findToken(taskId).orElseThrow(() -> EngineException.notFound("task"));
        requireLease(t, leaseOwner);
        t.leaseExpiresAt = System.currentTimeMillis() + extraMillis;
        t.updatedAt = System.currentTimeMillis();
        tx.updateToken(t);
        return t.leaseExpiresAt;
    }

    /** Leases dispatchable tokens to a worker and renders each as an activation. */
    List<TaskActivation> claim(Tx tx, String workerId, Set<String> queues, Set<WorkflowVersion> versions,
                               int max, long now, long until) {
        List<Token> claimed = tx.claimTasks(workerId, queues, versions, max, now, until);
        List<TaskActivation> activations = new ArrayList<>(claimed.size());
        for (Token t : claimed) {
            activationFor(tx, t, workerId, until).ifPresent(activations::add);
        }
        return activations;
    }

    private Optional<TaskActivation> activationFor(Tx tx, Token t, String workerId, long until) {
        Instance inst = tx.findInstance(t.instanceId).orElse(null);
        boolean comp = Sagas.isCompensation(t);
        if (inst == null || inst.status != (comp ? InstanceStatus.COMPENSATING : InstanceStatus.RUNNING)) {
            return Optional.empty();
        }
        if (comp) return Optional.of(Sagas.activation(inst, t, workerId, until));
        Node node = definitions.graph(tx, t.workflow, t.version).node(t.nodeId);
        ExecutionMode mode = resolveMode(definitions.executionMode(tx, t.workflow, t.version));
        Doc base = null;
        long itemIndex = 0;
        String itemMapKey = null;
        TokenPayload payload = t.payload;
        int at = payload.innermostItem();
        if (at >= 0) {
            TokenPayload.Frame item = payload.scopes().get(at);
            itemIndex = item.idx();
            itemMapKey = item.mapKey();
            base = Scopes.baseOf(inst, payload, at);
        }
        // A scoped combine's dispatched context is its staged inputs (over the scope view when
        // that view is an object); the pre-fork scope view itself travels as the base, so the
        // worker can hand it to @Context/Step.base() even when the view is a scalar.
        if (Scopes.isCombineNode(node) && payload.top() != null) {
            base = payload.top().view();
        }
        // An activation crosses the wire, so it carries plain trees: Doc stops here.
        return Optional.of(new TaskActivation(t.id, inst.id, inst.workflow, inst.version, node.id(), node.name(),
                node.activity(), node.kind(), t.attempt + 1, until, workerId, Scopes.dispatchContext(inst, t).raw(),
                base == null ? null : base.raw(), itemIndex, itemMapKey, mode));
    }

    /** DEFAULT resolves to the reference {@link ExecutionMode#SERVER} for now (no server-wide override yet). */
    private static ExecutionMode resolveMode(ExecutionMode mode) {
        return mode == null || mode == ExecutionMode.DEFAULT ? ExecutionMode.SERVER : mode;
    }

    /** What a failed token means beyond the token itself. */
    sealed interface Outcome {
        /** Rescheduled for another attempt; the instance is untouched. */
        record Retried() implements Outcome {}

        /** Out of retries: the token is FAILED. {@code compSeq} is non-null when it was a
         *  compensator, which the caller settles against the comp-log rather than the flow. */
        record Exhausted(String reason, Long compSeq) implements Outcome {}
    }

    /**
     * The shared retry-or-fail transition: bumps the attempt, releases the lease, then either
     * reschedules the token per the node's retry policy or marks it FAILED. What an exhausted
     * token means for the instance is the caller's call -- see {@link Outcome}.
     */
    Outcome retryOrFail(Tx tx, Instance inst, Token t, Node node,
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
            return new Outcome.Retried();
        }
        t.status = TokenStatus.FAILED;
        tx.updateToken(t);
        LOG.log(System.Logger.Level.DEBUG, () -> "fail: " + node.name() + " of instance " + inst.id
                + " exhausted retries (attempt " + t.attempt + "/" + policy.maxAttempts()
                + ", retryable=" + retryable + ") -> failing instance");
        return new Outcome.Exhausted(failReason, Sagas.seqOf(t));
    }

    /** Marks a parked token consumed ahead of minting its continuation. Used by the leader sweeps,
     *  where the token is WAITING or AWAITING and so holds no lease to release. */
    static void discard(Tx tx, Token t, long now) {
        t.status = TokenStatus.DONE;
        t.updatedAt = now;
        tx.updateToken(t);
    }
}
