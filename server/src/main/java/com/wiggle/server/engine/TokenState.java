package com.wiggle.server.engine;

import com.wiggle.server.store.Rows.Token;
import com.wiggle.server.store.Rows.TokenStatus;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * What a token in each state IS, and what it PERMITS. One constant per {@link TokenStatus},
 * matched by name.
 *
 * <p>This type answers questions; it never performs a write. Nothing here takes a {@code Tx}, so
 * a state cannot advance a token -- it can only say whether a move is legal ({@link #moveTo}),
 * whether the token still counts as live ({@link #active}), and whether an operation that needs a
 * lease is allowed at all. {@link Tokens} does the writing, and asks here first. That division is
 * the whole boundary between the two: <em>state answers, lifecycle writes</em>.
 *
 * <p>Only {@code RUNNING} overrides anything, which is the honest shape of this machine: holding a
 * lease is the one thing a state can do that the others cannot, so it is the one thing worth
 * dispatching on.
 */
enum TokenState {

    READY(Liveness.ACTIVE, TokenStatus.RUNNING, TokenStatus.WAITING, TokenStatus.AWAITING,
            TokenStatus.JOINED, TokenStatus.DONE, TokenStatus.CANCELLED),

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
    },

    WAITING(Liveness.ACTIVE, TokenStatus.DONE, TokenStatus.CANCELLED),
    AWAITING(Liveness.ACTIVE, TokenStatus.DONE, TokenStatus.FAILED, TokenStatus.CANCELLED),
    JOINED(Liveness.ACTIVE, TokenStatus.DONE, TokenStatus.CANCELLED),
    DONE(Liveness.SETTLED),
    FAILED(Liveness.SETTLED),
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

    /** Implies a non-null {@code leaseOwner} and an expiry. Only RUNNING. */
    boolean holdsLease() {
        return false;
    }

    /** The states this one may become; empty for a settled state. Read by the chart that
     *  generates {@code docs/state-machines.md}, so the diagram cannot drift from the machine. */
    Set<TokenStatus> successors() {
        return successors;
    }

    /** Drops the lease, for the one state that has one to drop. A no-op everywhere else, which is
     *  what lets every settling transition in {@link Tokens} be written the same way. */
    void releaseLease(Token t) {
    }

    void renewLease(Token t, long until) {
        throw EngineException.conflict("task " + t.id + " is " + t.status + ", not RUNNING");
    }

    /** Rejects an operation that needs this worker's lease. Only RUNNING accepts one; passing a
     *  null owner asks the weaker question, "is this token leased at all". */
    void requireLeasedBy(Token t, String leaseOwner) {
        throw EngineException.conflict("task " + t.id + " is " + t.status + ", not RUNNING");
    }

    /**
     * {@code target}, if this state may become it -- otherwise a failure, so an impossible token is
     * refused rather than persisted. Staying put is always allowed: an update that rewrites a
     * token's node bookkeeping without changing its state is not a transition.
     */
    TokenStatus moveTo(TokenStatus target) {
        if (target != TokenStatus.valueOf(name()) && !successors.contains(target)) {
            throw new IllegalStateException("illegal token transition " + name() + " -> " + target
                    + "; " + name() + " may only become " + successors);
        }
        return target;
    }
}
