package com.wiggle.server.engine;

import com.wiggle.core.Ids;
import com.wiggle.core.Node;
import com.wiggle.core.NodeKind;
import com.wiggle.core.RetryPolicy;
import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.Rows.Token;
import com.wiggle.server.store.Rows.TokenStatus;
import com.wiggle.server.store.TokenPayload;
import com.wiggle.server.store.Tx;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

enum TokenState {

    READY(Liveness.ACTIVE, TokenStatus.RUNNING, TokenStatus.WAITING, TokenStatus.AWAITING,
            TokenStatus.JOINED, TokenStatus.DONE, TokenStatus.CANCELLED) {
        @Override boolean claimable() { return true; }
    },

    RUNNING(Liveness.ACTIVE, TokenStatus.DONE, TokenStatus.READY, TokenStatus.FAILED,
            TokenStatus.CANCELLED) {
        @Override boolean holdsLease() { return true; }

        @Override void releaseLease(Token t) {
            t.leaseOwner = null;
            t.leaseExpiresAt = 0;
        }

        @Override void renewLease(Token t, long until) {
            t.leaseExpiresAt = until;
        }

        @Override void requireLeasedBy(Token t, String leaseOwner) {
            if (leaseOwner != null && !leaseOwner.equals(t.leaseOwner)) {
                throw EngineException.conflict("lease for task " + t.id + " is held by " + t.leaseOwner);
            }
        }

        @Override Outcome reportFailure(Tx tx, Token t, Node node, String lastError,
                                        String failReason, boolean retryable, long now) {
            RetryPolicy policy = node.retry() == null ? RetryPolicy.forever() : node.retry();
            t.attempt++;
            t.lastError = lastError;
            releaseLease(t);
            if (retryable && t.attempt < policy.maxAttempts()) {
                t.availableAt = now + policy.backoffMillis(t.attempt);
                move(tx, t, TokenStatus.READY, now);
                return new Outcome.Retried();
            }
            move(tx, t, TokenStatus.FAILED, now);
            return new Outcome.Exhausted(failReason, Sagas.seqOf(t));
        }
    },
    WAITING(Liveness.ACTIVE, TokenStatus.DONE, TokenStatus.CANCELLED),
    AWAITING(Liveness.ACTIVE, TokenStatus.DONE, TokenStatus.FAILED, TokenStatus.CANCELLED),
    JOINED(Liveness.ACTIVE, TokenStatus.DONE, TokenStatus.CANCELLED),
    DONE(Liveness.SETTLED),
    FAILED(Liveness.SETTLED),
    CANCELLED(Liveness.SETTLED);

    private static final System.Logger LOG = System.getLogger(TokenState.class.getName());

    private enum Liveness { ACTIVE, SETTLED }

    private final Liveness liveness;
    private final Set<TokenStatus> successors;

    TokenState(Liveness liveness, TokenStatus... successors) {
        this.liveness = liveness;
        this.successors = successors.length == 0
                ? Collections.unmodifiableSet(EnumSet.noneOf(TokenStatus.class))
                : Collections.unmodifiableSet(EnumSet.copyOf(Set.of(successors)));
    }

    static TokenState of(TokenStatus status) {
        return valueOf(status.name());
    }

    static { for (TokenStatus s : TokenStatus.values()) of(s); }   // every status has a state, or fail at load

    boolean active() {
        return liveness == Liveness.ACTIVE;
    }

    boolean claimable() {
        return false;
    }

    boolean holdsLease() {
        return false;
    }

    Set<TokenStatus> successors() {
        return successors;
    }

    void releaseLease(Token t) {
    }

    void renewLease(Token t, long until) {
        throw EngineException.conflict("task " + t.id + " is " + t.status + ", not RUNNING");
    }

    void requireLeasedBy(Token t, String leaseOwner) {
        throw EngineException.conflict("task " + t.id + " is " + t.status + ", not RUNNING");
    }

    sealed interface Outcome {
        record Retried() implements Outcome {}

        record Exhausted(String reason, Long compSeq) implements Outcome {}
    }

    Outcome reportFailure(Tx tx, Token t, Node node, String lastError,
                          String failReason, boolean retryable, long now) {
        throw EngineException.conflict("task " + t.id + " is " + t.status + ", not RUNNING");
    }

    TokenStatus moveTo(TokenStatus target) {
        if (target != TokenStatus.valueOf(name()) && !successors.contains(target)) {
            throw new IllegalStateException("illegal token transition " + name() + " -> " + target
                    + "; " + name() + " may only become " + successors);
        }
        return target;
    }

    private static void move(Tx tx, Token t, TokenStatus target, long now) {
        t.status = of(t.status).moveTo(target);
        t.updatedAt = now;
        tx.updateToken(t);
    }

    static Token create(Instance inst, String nodeId, String joinStack, TokenPayload payload, long now) {
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

    /**
     * The continuation of {@code t} at {@code nextNodeId}, inserted and ready to drive. It inherits
     * {@code t}'s join stack, which is what keeps a token inside the fork group it was spawned in.
     *
     * <p>Use {@link #create} directly only where that is deliberately not what should happen: an
     * instance's first token belongs to no group, a join's continuation pops the group it just
     * satisfied, and a locally-chained run leases its continuation back rather than inserting it
     * READY ({@link #createLeased}).
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
        cont.updatedAt = now;
        tx.insertToken(cont);
    }

    static void markReady(Tx tx, Token t, Node node, long now) {
        t.kind = node.kind();
        t.activity = node.activity();
        t.queue = node.queue();
        t.availableAt = now;
        move(tx, t, TokenStatus.READY, now);
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
        TokenStatus before = t.status;
        of(before).releaseLease(t);
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
        of(t.status).releaseLease(t);
        move(tx, t, TokenStatus.FAILED, now);
    }

    static void cancel(Tx tx, Token t, long now) {
        TokenStatus before = t.status;
        of(before).releaseLease(t);
        move(tx, t, TokenStatus.CANCELLED, now);
    }
}
