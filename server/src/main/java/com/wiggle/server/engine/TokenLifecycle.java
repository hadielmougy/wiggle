package com.wiggle.server.engine;

import com.wiggle.core.Doc;
import com.wiggle.core.ExecutionMode;
import com.wiggle.core.Node;
import com.wiggle.core.TaskActivation;
import com.wiggle.core.WorkflowVersion;
import com.wiggle.server.store.Rows.Instance;
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

    /** Parks a token for a worker to claim, then marks its queue for the post-commit wake. */
    void parkReady(Tx tx, Instance inst, Token t, Node node, long now) {
        TokenStatus before = TokenState.parkReady(tx, t, node, now);
        transactions.wake(node.queue());
        LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                + node.name() + " (" + node.kind() + ") " + before + " -> READY, queue=" + node.queue());
    }

    /** Marks a queue for the post-commit wake, for a token inserted READY rather than parked
     *  there -- the saga reverse pass mints its undo tasks that way. */
    void wake(String queue) {
        transactions.wake(queue);
    }

    /** Cancels every token of an instance that is still active. */
    static void cancelAll(Tx tx, String instanceId, long now) {
        for (Token t : tx.tokensOf(instanceId)) {
            if (!TokenState.of(t.status).active() || t.id == null) continue;
            TokenStatus before = TokenState.cancel(tx, t, now);
            LOG.log(System.Logger.Level.DEBUG, () -> "cancelActiveTokens: " + instanceId + " token " + t.id
                    + " at " + t.nodeId + " " + before + " -> CANCELLED");
        }
    }

    /** Whether any token of the instance is still active -- what decides an END node completes it. */
    static boolean anyActive(Tx tx, String instanceId) {
        return tx.tokensOf(instanceId).stream().anyMatch(t -> TokenState.of(t.status).active());
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
        TokenState.of(t.status).requireLeasedBy(t, leaseOwner);
    }

    /** Extends the lease of an in-flight task (worker heartbeat for long-running steps). */
    static long extendLease(Tx tx, String taskId, String leaseOwner, long extraMillis) {
        Token t = tx.findToken(taskId).orElseThrow(() -> EngineException.notFound("task"));
        requireLease(t, leaseOwner);
        long now = System.currentTimeMillis();
        TokenState.of(t.status).renewLease(t, now + extraMillis);
        t.updatedAt = now;
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
        if (inst == null || !InstanceState.of(inst.status).dispatches(comp)) {
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

}
