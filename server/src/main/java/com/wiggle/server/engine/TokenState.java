package com.wiggle.server.engine;

import com.wiggle.server.store.Rows.Token;
import com.wiggle.server.store.Rows.TokenStatus;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * What a token in each state is and may become. One constant per {@link TokenStatus}, matched by
 * name -- the same shape {@link NodeBehaviours} uses for node kinds.
 *
 * <p>Reading one constant tells you everything about that state: whether it counts as active,
 * whether a worker may claim it, whether it holds a lease, and every state it may move to.
 * {@link TokenLifecycle} routes all its writes through {@link #moveTo}, so an illegal move throws
 * here instead of quietly corrupting an instance.
 *
 * <p>The one transition this class does not police is {@code READY -> RUNNING}: a claim has to be
 * atomic with the {@code SKIP LOCKED} select that finds the token, so the store performs it.
 */
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

        @Override void requireLeasedBy(Token t, String leaseOwner) {
            if (leaseOwner != null && !leaseOwner.equals(t.leaseOwner)) {
                throw EngineException.conflict("lease for task " + t.id + " is held by " + t.leaseOwner);
            }
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

    /** Rejects a report against a token this worker does not hold. Only RUNNING accepts one. */
    void requireLeasedBy(Token t, String leaseOwner) {
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
}
