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

    /** Dispatchable to a worker. A retry waits here too, behind {@code availableAt}. */
    READY(Liveness.ACTIVE, TokenStatus.RUNNING, TokenStatus.WAITING, TokenStatus.AWAITING,
            TokenStatus.JOINED, TokenStatus.DONE, TokenStatus.CANCELLED) {
        @Override boolean claimable() { return true; }
    },

    /** Leased by a worker, which owes a result, a failure, or a heartbeat. */
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
                long backoffMs = t.availableAt - now;
                LOG.log(System.Logger.Level.DEBUG, () -> "fail: " + node.name() + " of instance "
                        + t.instanceId + " failed (" + lastError + "), retrying attempt " + t.attempt
                        + "/" + policy.maxAttempts() + " in " + backoffMs + "ms");
                return new Outcome.Retried();
            }
            move(tx, t, TokenStatus.FAILED, now);
            LOG.log(System.Logger.Level.DEBUG, () -> "fail: " + node.name() + " of instance "
                    + t.instanceId + " exhausted retries (attempt " + t.attempt + "/"
                    + policy.maxAttempts() + ", retryable=" + retryable + ") -> failing instance");
            return new Outcome.Exhausted(failReason, Sagas.seqOf(t));
        }
    },

    /** Sleeping until {@code availableAt}; the leader's timer sweep wakes it. */
    WAITING(Liveness.ACTIVE, TokenStatus.DONE, TokenStatus.CANCELLED),

    /** Parked on an external actor: a signal, or a child instance. */
    AWAITING(Liveness.ACTIVE, TokenStatus.DONE, TokenStatus.FAILED, TokenStatus.CANCELLED),

    /** Parked at a join barrier, waiting on its siblings. */
    JOINED(Liveness.ACTIVE, TokenStatus.DONE, TokenStatus.CANCELLED),

    /** Consumed: a completed step, a spent fork, a satisfied barrier. */
    DONE(Liveness.SETTLED),

    /** Out of retries, or waiting on something that can no longer arrive. */
    FAILED(Liveness.SETTLED),

    /** Abandoned because the instance stopped running. */
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

    /** Still doing something, or waiting to: an instance with one of these has not finished. */
    boolean active() {
        return liveness == Liveness.ACTIVE;
    }

    /** A worker may lease it. Only READY. */
    boolean claimable() {
        return false;
    }

    /** Implies a non-null {@code leaseOwner} and an expiry. Only RUNNING. */
    boolean holdsLease() {
        return false;
    }

    /** The states this one may move to; empty for a settled state. */
    Set<TokenStatus> successors() {
        return successors;
    }

    /** Drops the lease, for the one state that has one to drop. A no-op everywhere else, which is
     *  what lets every settling transition below be written the same way. */
    void releaseLease(Token t) {
    }

    /** Extends the lease, for the one state that has one to extend. */
    void renewLease(Token t, long until) {
        throw EngineException.conflict("task " + t.id + " is " + t.status + ", not RUNNING");
    }

    /** Rejects a report against a token this worker does not hold. Only RUNNING accepts one. */
    void requireLeasedBy(Token t, String leaseOwner) {
        throw EngineException.conflict("task " + t.id + " is " + t.status + ", not RUNNING");
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
     * The retry-or-fail transition, reported by a worker or forced by an expired lease: bumps the
     * attempt, releases the lease, then either reschedules per the node's policy or fails the
     * token. What an exhausted token means for its instance is the caller's call. Only a leased
     * token can fail; anywhere else this is a conflict.
     */
    Outcome reportFailure(Tx tx, Token t, Node node, String lastError,
                          String failReason, boolean retryable, long now) {
        throw EngineException.conflict("task " + t.id + " is " + t.status + ", not RUNNING");
    }

    /**
     * The status a token in this state takes on moving to {@code target}, or a failure when that
     * move is not one this state allows. Staying put is always allowed -- an update that rewrites
     * a token's node bookkeeping without changing its state is not a transition.
     */
    TokenStatus moveTo(TokenStatus target) {
        if (target != TokenStatus.valueOf(name()) && !successors.contains(target)) {
            throw new IllegalStateException("illegal token transition " + name() + " -> " + target
                    + "; " + name() + " may only become " + successors);
        }
        return target;
    }

    /** The one place a token's status is written. */
    private static void move(Tx tx, Token t, TokenStatus target, long now) {
        t.status = of(t.status).moveTo(target);
        t.updatedAt = now;
        tx.updateToken(t);
    }

    /** A fresh READY token for {@code inst} at {@code nodeId}. Not yet inserted. */
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

    /** Inserts a continuation already leased to the worker that reported its predecessor, so a
     *  local run keeps the chain instead of returning it to {@link Dispatch#poll}. */
    static void mintLeased(Tx tx, Token cont, Node nextNode, String leaseOwner, long lease, long now) {
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

    /** Parks a token for a worker to claim. The caller wakes the queue once the transaction commits. */
    static TokenStatus parkReady(Tx tx, Token t, Node node, long now) {
        TokenStatus before = t.status;
        t.kind = node.kind();
        t.activity = node.activity();
        t.queue = node.queue();
        t.availableAt = now;
        move(tx, t, TokenStatus.READY, now);
        return before;
    }

    /** Parks a token on the clock until {@code availableAt}. */
    static TokenStatus parkWaiting(Tx tx, Token t, long availableAt, long now) {
        TokenStatus before = t.status;
        t.kind = NodeKind.SLEEP;
        t.availableAt = availableAt;
        move(tx, t, TokenStatus.WAITING, now);
        return before;
    }

    /** Parks a token on an external actor: a signal, or a child instance. A positive
     *  {@code deadline} is the (optional) time the leader sweeps it at. */
    static TokenStatus parkAwaiting(Tx tx, Token t, NodeKind kind, String activity, long deadline, long now) {
        TokenStatus before = t.status;
        t.kind = kind;
        t.activity = activity;
        t.availableAt = deadline;
        move(tx, t, TokenStatus.AWAITING, now);
        return before;
    }

    /** Parks a token at a join barrier, waiting on its siblings. */
    static TokenStatus parkJoined(Tx tx, Token t, long now) {
        TokenStatus before = t.status;
        t.kind = NodeKind.JOIN;
        move(tx, t, TokenStatus.JOINED, now);
        return before;
    }

    /** Consumes a token: a completed step, a fired timer, a delivered signal, a satisfied barrier.
     *  Any lease it held goes with it. */
    static TokenStatus settle(Tx tx, Token t, long now) {
        TokenStatus before = t.status;
        of(before).releaseLease(t);
        move(tx, t, TokenStatus.DONE, now);
        return before;
    }

    /** {@link #settle} for the whole of a satisfied barrier. */
    static void settleAll(Tx tx, List<Token> tokens, long now) {
        for (Token t : tokens) settle(tx, t, now);
    }

    /** Consumes a token at a node that spends it outright -- a fork that has spawned its branches,
     *  or an END that has run out of flow -- recording the kind it ended at. */
    static TokenStatus spend(Tx tx, Token t, NodeKind kind, long now) {
        t.kind = kind;
        return settle(tx, t, now);
    }

    /** Fails a parked token outright, with no retry: the thing it was waiting on can no longer
     *  arrive. A sub-workflow that ended badly leaves its parent's token this way. */
    static void failParked(Tx tx, Token t, long now) {
        of(t.status).releaseLease(t);
        move(tx, t, TokenStatus.FAILED, now);
    }

    /** Abandons a token because its instance stopped running. */
    static TokenStatus cancel(Tx tx, Token t, long now) {
        TokenStatus before = t.status;
        of(before).releaseLease(t);
        move(tx, t, TokenStatus.CANCELLED, now);
        return before;
    }
}
