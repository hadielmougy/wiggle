package com.wiggle.server.engine;

import com.wiggle.core.Doc;
import com.wiggle.core.ExecutionMode;
import com.wiggle.core.Node;
import com.wiggle.core.NodeKind;
import com.wiggle.core.RetryPolicy;
import com.wiggle.core.ShardIds;
import com.wiggle.core.TaskActivation;
import com.wiggle.core.WorkflowVersion;
import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.Rows.LockedTask;
import com.wiggle.server.store.Rows.Token;
import com.wiggle.core.TokenStatus;
import com.wiggle.server.store.TokenPayload;
import com.wiggle.server.store.Tx;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;


/**
 * Everything that writes a token. {@link TokenState} says what each state is and what it permits;
 * this performs the moves, asking there first -- every status write goes through {@link #move},
 * which refuses a transition no state declares.
 *
 * <p>The split is by responsibility, not by whether a method happens to need a field:
 * <em>state answers, lifecycle writes</em>. Anything here that takes a {@code Tx} belongs here;
 * anything that only inspects a token belongs next door.
 */
final class Tokens {

    private final DefinitionRegistry definitions;
    private final QueueWake queueWake;

    Tokens(DefinitionRegistry definitions, QueueWake queueWake) {
        this.definitions = definitions;
        this.queueWake = queueWake;
    }

    void wake(String queue) {
        queueWake.ready(queue);
    }

    /** The one place a token's status is written. */
    private static void move(Tx tx, Token t, TokenStatus target, long now) {
        t.status = TokenState.of(t.status).moveTo(target);
        t.updatedAt = now;
        tx.updateToken(t);
    }

    static Token create(Instance inst, String nodeId, String joinStack, TokenPayload payload, long now) {
        Token t = new Token();
        t.payload = payload == null ? TokenPayload.EMPTY : payload;
        t.id = ShardIds.inherit("tok", inst.id);
        t.instanceId = inst.id;
        t.workflow = inst.workflow;
        t.version = inst.version;
        t.nodeId = nodeId;
        t.kind = NodeKind.TASK;
        t.status = TokenStatus.READY;
        t.attempt = 0;
        t.availableAt = now;
        t.instCreatedAt = inst.createdAt;
        t.joinStack = joinStack == null ? "" : joinStack;
        t.createdAt = now;
        t.updatedAt = now;
        return t;
    }

    /**
     * The continuation of {@code t} at {@code nextNodeId}, inserted and ready to drive. It inherits
     * {@code t}'s join stack, which is what keeps a token inside the fork group it was spawned in.
     *
     * <p>Use {@link #create} directly only where that is deliberately not what should happen: an
     * instance's first token belongs to no group, a join's continuation pops the group it just
     * satisfied, and a locally-chained run leases its continuation back ({@link #createLeased}).
     */
    static Token continueAt(Tx tx, Instance inst, Token t, String nextNodeId, TokenPayload payload, long now) {
        Token cont = create(inst, nextNodeId, t.joinStack, payload, now);
        tx.insertToken(cont);
        return cont;
    }

    static void createLeased(Tx tx, Token cont, Node nextNode, String leaseOwner, long lease, long now) {
        cont.status = TokenStatus.RUNNING;
        cont.kind = nextNode.kind();
        cont.activity = nextNode.activity();
        cont.queue = nextNode.queue();
        cont.leaseOwner = leaseOwner;
        cont.leaseExpiresAt = lease;
        cont.availableAt = now;
        cont.startedAt = now;   // handed straight to the worker: no wait, and its clock starts now
        cont.updatedAt = now;
        tx.insertToken(cont);
    }

    void markReady(Tx tx, Token t, Node node, long now) {
        t.kind = node.kind();
        t.activity = node.activity();
        t.queue = node.queue();
        t.availableAt = now;
        move(tx, t, TokenStatus.READY, now);
        wake(node.queue());
    }

    static void markWaiting(Tx tx, Token t, long availableAt, long now) {
        t.kind = NodeKind.SLEEP;
        t.availableAt = availableAt;
        move(tx, t, TokenStatus.WAITING, now);
    }

    static void markAwaiting(Tx tx, Token t, NodeKind kind, String activity, long deadline, long now) {
        t.kind = kind;
        t.activity = activity;
        t.availableAt = deadline;
        move(tx, t, TokenStatus.AWAITING, now);
    }

    static void markJoined(Tx tx, Token t, long now) {
        t.kind = NodeKind.JOIN;
        move(tx, t, TokenStatus.JOINED, now);
    }

    static void settle(Tx tx, Token t, long now) {
        TokenState.of(t.status).releaseLease(t);
        if (t.startedAt != null && t.finishedAt == null) t.finishedAt = now;   // a clock the server started, it closes
        move(tx, t, TokenStatus.DONE, now);
    }

    static void settleAll(Tx tx, List<Token> tokens, long now) {
        for (Token t : tokens) settle(tx, t, now);
    }

    static void spend(Tx tx, Token t, NodeKind kind, long now) {
        t.kind = kind;
        settle(tx, t, now);
    }

    static void failParked(Tx tx, Token t, long now) {
        TokenState.of(t.status).releaseLease(t);
        move(tx, t, TokenStatus.FAILED, now);
    }

    static void cancel(Tx tx, Token t, long now) {
        TokenState.of(t.status).releaseLease(t);
        move(tx, t, TokenStatus.CANCELLED, now);
    }

    /** What a failed token means beyond the token itself. */
    sealed interface Outcome {
        record Retried() implements Outcome {}

        record Exhausted(String reason, Long compSeq) implements Outcome {}
    }

    /**
     * The retry-or-fail transition. Only a leased token can fail, which the state is asked to
     * confirm before anything is written; what an exhausted token means for its instance is the
     * caller's call. A retry whose backoff is at least {@code parkFromMillis} waits WAITING, out of
     * the dispatch index, until {@link #promote} makes it READY; a shorter one waits READY.
     */
    static Outcome reportFailure(Tx tx, Token t, Node node, String lastError,
                                 String failReason, boolean retryable, long parkFromMillis, long now) {
        TokenState state = TokenState.of(t.status);
        state.requireLeasedBy(t, null);
        RetryPolicy policy = node.retry() == null ? RetryPolicy.forever() : node.retry();
        t.attempt++;
        t.lastError = lastError;
        state.releaseLease(t);
        if (retryable && t.attempt < policy.maxAttempts()) {
            long backoff = policy.backoffMillis(t.attempt);
            t.availableAt = now + backoff;
            move(tx, t, backoff >= parkFromMillis ? TokenStatus.WAITING : TokenStatus.READY, now);
            return new Outcome.Retried();
        }
        move(tx, t, TokenStatus.FAILED, now);
        return new Outcome.Exhausted(failReason, Sagas.seqOf(t));
    }

    /** A parked retry whose backoff has run out becomes dispatchable, and its queue is woken. */
    void promote(Tx tx, Token t, long now) {
        move(tx, t, TokenStatus.READY, now);
        wake(t.queue);
    }

    /** Cancels every still-active token of an instance in one statement. The per-token
     *  {@link #cancel} path stays for the callers that hold a token already. */
    static void cancelAll(Tx tx, String instanceId, long now) {
        tx.cancelActiveTokens(instanceId, now);
    }

    static boolean anyActive(Tx tx, String instanceId) {
        return tx.hasActiveTokens(instanceId);
    }

    /** The task's instance, locked, and the task as it stands under that lock. */
    static LockedTask lock(Tx tx, String taskId) {
        return tx.lockTask(taskId).orElseThrow(() -> tx.findToken(taskId).isPresent()
                ? EngineException.notFound("instance") : EngineException.notFound("task"));
    }

    static void requireLease(Token t, String leaseOwner) {
        TokenState.of(t.status).requireLeasedBy(t, leaseOwner);
    }

    /** Renews in one write; only a refused renewal reads the token, to say why it was refused. */
    static long extendLease(Tx tx, String taskId, String leaseOwner, long extraMillis) {
        long now = System.currentTimeMillis();
        long until = now + extraMillis;
        if (tx.renewLease(taskId, leaseOwner, until, now)) return until;
        Token t = tx.findToken(taskId).orElseThrow(() -> EngineException.notFound("task"));
        requireLease(t, leaseOwner);
        TokenState.of(t.status).renewLease(t, until);
        throw EngineException.conflict("lease for task " + taskId + " changed while renewing it");
    }

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
        ExecutionMode mode = ExecutionModes.resolve(definitions.executionMode(tx, t.workflow, t.version));
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
        if (Scopes.isCombineNode(node) && payload.top() != null) {
            base = payload.top().view();
        }
        return Optional.of(new TaskActivation(t.id, inst.id, inst.workflow, inst.version, node.id(), node.name(),
                node.activity(), node.kind(), t.nextAttempt(), until, workerId, Scopes.dispatchContext(inst, t).raw(),
                base == null ? null : base.raw(), itemIndex, itemMapKey, mode));
    }




}
