package com.wiggle.core;

/**
 * The status a token is persisted with, and the one authority on whether a token in that status is
 * still doing something. Shared by the engine and the stores, so the active set is defined once.
 */
public enum TokenStatus {

    /** Dispatchable to a worker. */             READY(true),
    /** Leased by a worker. */                   RUNNING(true),
    /** Sleeping until availableAt. */           WAITING(true),
    /** Awaiting an external/user completion. */ AWAITING(true),
    /** Parked at a join barrier. */             JOINED(true),
    /** Consumed. */                             DONE(false),
    /** Terminally failed. */                    FAILED(false),
    /** Abandoned because a sibling failed. */   CANCELLED(false);

    private final boolean active;

    TokenStatus(boolean active) {
        this.active = active;
    }

    /** Still doing something, or waiting to: an instance with one of these has not finished. */
    public boolean active() {
        return active;
    }
}
