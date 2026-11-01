package com.wiggle.server.engine;

import com.wiggle.server.store.Repositories;
import com.wiggle.core.Doc;
import com.wiggle.core.ExecutionMode;
import com.wiggle.core.Node;
import com.wiggle.core.criteria.DispatchAttribute;
import com.wiggle.core.NodeKind;
import com.wiggle.core.RetryPolicy;
import com.wiggle.core.ShardIds;
import com.wiggle.core.TaskActivation;
import com.wiggle.core.WorkflowVersion;
import com.wiggle.server.store.ClaimFilter;
import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.Rows.LockedTask;
import com.wiggle.server.store.Rows.Token;
import com.wiggle.core.TokenStatus;
import com.wiggle.server.store.TokenPayload;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;


/**
 * Everything that writes a token. {@link TokenState} says what each state is and what it permits;
 * this performs the moves, asking there first -- every status write goes through {@link #move},
 * which refuses a transition no state declares.
 *
 * <p>The split is by responsibility, not by whether a method happens to need a field:
 * <em>state answers, lifecycle writes</em>. Anything here that writes through the repositories belongs here;
 * anything that only inspects a token belongs next door.
 */
final class Tokens {

    private final DefinitionRegistry definitions;
    private final QueueWake queueWake;

    Tokens(DefinitionRegistry definitions, QueueWake queueWake) {
        this.definitions = definitions;
        this.queueWake = queueWake;
    }

    /**
     * Marks {@code t}'s queue as having gained work of {@code t}'s version, with its facts for parked
     * polls' criteria. The facts are left unknown when the flow declares attributes this token's
     * copy does not carry (a token read back from a store that does not load them).
     */
    void wake(Token t) {
        boolean factsKnown = t.attributes != null || definitions.attributes(t.workflow, t.version).isEmpty();
        queueWake.ready(t.queue, new WorkflowVersion(t.workflow, t.version), factsKnown ? ClaimFilter.facts(t) : null);
    }

    /** The one place a token's status is written. */
    private static void move(Token t, TokenStatus target, long now) {
        t.status = TokenState.of(t.status).moveTo(target);
        t.updatedAt = now;
        Repositories.tokens().updateToken(t);
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
        t.correlationId = inst.correlationId;
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
    static Token continueAt(Instance inst, Token t, String nextNodeId, TokenPayload payload, long now) {
        Token cont = create(inst, nextNodeId, t.joinStack, payload, now);
        Repositories.tokens().insertToken(cont);
        return cont;
    }

    static void createLeased(Token cont, Node nextNode, String leaseOwner, long lease, long now) {
        cont.status = TokenStatus.RUNNING;
        cont.kind = nextNode.kind();
        cont.activity = nextNode.activity();
        cont.queue = nextNode.queue();
        cont.leaseOwner = leaseOwner;
        cont.leaseExpiresAt = lease;
        cont.availableAt = now;
        cont.startedAt = now;   // handed straight to the worker: no wait, and its clock starts now
        cont.updatedAt = now;
        Repositories.tokens().insertToken(cont);
    }

    void markReady(Instance inst, Token t, Node node, long now) {
        t.kind = node.kind();
        t.activity = node.activity();
        t.queue = node.queue();
        t.availableAt = now;
        stampAttributes(definitions, inst, t);
        move(t, TokenStatus.READY, now);
        wake(t);
    }

    /**
     * Reads the flow's declared attributes from the context {@code t}'s step receives, to be written
     * with the token. A flow that declares none costs a cache lookup and writes nothing.
     */
    static void stampAttributes(DefinitionRegistry definitions, Instance inst, Token t) {
        List<DispatchAttribute> declared = definitions.attributes(t.workflow, t.version);
        if (declared.isEmpty()) return;
        t.attributes = DispatchAttribute.extractAll(declared, Scopes.dispatchContext(inst, t).raw());
        t.attributesChanged = true;
    }

    static void markWaiting(Token t, long availableAt, long now) {
        t.kind = NodeKind.SLEEP;
        t.availableAt = availableAt;
        move(t, TokenStatus.WAITING, now);
    }

    static void markAwaiting(Token t, NodeKind kind, String activity, long deadline, long now) {
        t.kind = kind;
        t.activity = activity;
        t.availableAt = deadline;
        move(t, TokenStatus.AWAITING, now);
    }

    static void markJoined(Token t, long now) {
        t.kind = NodeKind.JOIN;
        move(t, TokenStatus.JOINED, now);
    }

    static void settle(Token t, long now) {
        TokenState.of(t.status).releaseLease(t);
        if (t.startedAt != null && t.finishedAt == null) t.finishedAt = now;   // a clock the server started, it closes
        move(t, TokenStatus.DONE, now);
    }

    static void settleAll(List<Token> tokens, long now) {
        for (Token t : tokens) settle(t, now);
    }

    static void spend(Token t, NodeKind kind, long now) {
        t.kind = kind;
        settle(t, now);
    }

    static void failParked(Token t, long now) {
        TokenState.of(t.status).releaseLease(t);
        move(t, TokenStatus.FAILED, now);
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
    static Outcome reportFailure(Token t, Node node, String lastError,
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
            move(t, backoff >= parkFromMillis ? TokenStatus.WAITING : TokenStatus.READY, now);
            return new Outcome.Retried();
        }
        move(t, TokenStatus.FAILED, now);
        return new Outcome.Exhausted(failReason, Sagas.seqOf(t));
    }

    /** A parked retry whose backoff has run out becomes dispatchable, and its queue is woken. */
    void promote(Token t, long now) {
        move(t, TokenStatus.READY, now);
        wake(t);
    }

    static void cancelAll(String instanceId, long now) {
        Repositories.tokens().cancelActiveTokens(instanceId, now);
    }

    static boolean anyActive(String instanceId) {
        return Repositories.tokens().hasActiveTokens(instanceId);
    }

    /** The task's instance, locked, and the task as it stands under that lock. */
    static LockedTask lock(String taskId) {
        return Repositories.locks().lockTask(taskId).orElseThrow(() -> Repositories.locks().findToken(taskId).isPresent()
                ? EngineException.notFound("instance") : EngineException.notFound("task"));
    }

    /** Refuses, as if there were no such task, a caller whose {@code queues} (null: every queue) miss the task's. */
    static void requireQueue(Token t, Predicate<String> queues) {
        if (queues != null && !queues.test(t.queue)) throw EngineException.notFound("task");
    }

    static void requireLease(Token t, String leaseOwner) {
        TokenState.of(t.status).requireLeasedBy(t, leaseOwner);
    }

    /**
     * Renews in one write; only a refused renewal reads the token, to say why it was refused. A
     * caller limited to some {@code queues} reads it first, to check the task is on one of them.
     */
    static long extendLease(String taskId, String leaseOwner, long extraMillis, Predicate<String> queues) {
        if (queues != null) requireQueue(Repositories.locks().findToken(taskId).orElseThrow(() -> EngineException.notFound("task")), queues);
        long now = System.currentTimeMillis();
        long until = now + extraMillis;
        if (Repositories.tokens().renewLease(taskId, leaseOwner, until, now)) return until;
        Token t = Repositories.locks().findToken(taskId).orElseThrow(() -> EngineException.notFound("task"));
        requireLease(t, leaseOwner);
        TokenState.of(t.status).renewLease(t, until);
        throw EngineException.conflict("lease for task " + taskId + " changed while renewing it");
    }

    List<TaskActivation> claim(String workerId, ClaimFilter filter, int max, long now, long until) {
        List<Token> claimed = Repositories.tokens().claimTasks(workerId, filter, max, now, until);
        List<TaskActivation> activations = new ArrayList<>(claimed.size());
        for (Token t : claimed) {
            activationFor(t, workerId, until).ifPresent(activations::add);
        }
        return activations;
    }

    private Optional<TaskActivation> activationFor(Token t, String workerId, long until) {
        Instance inst = Repositories.instances().findInstance(t.instanceId).orElse(null);
        boolean comp = Sagas.isCompensation(t);
        if (inst == null || !InstanceState.of(inst.status).dispatches(comp)) {
            return Optional.empty();
        }
        if (comp) return Optional.of(Sagas.activation(inst, t, workerId, until));
        Node node = definitions.graph(t.workflow, t.version).node(t.nodeId);
        // A created node is in no worker's compiled graph, so it runs one step at a time.
        ExecutionMode mode = Spawns.isCreated(node.id()) ? ExecutionMode.SERVER
                : ExecutionModes.resolve(definitions.executionMode(t.workflow, t.version));
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
                base == null ? null : base.raw(), itemIndex, itemMapKey, mode, node.collectKey(),
                node.armNames(), inst.correlationId, inst.createdAt));
    }




}
