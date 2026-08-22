package dev.wiggle.core;

public enum NodeKind {
    /** Executed by a worker; the returned object is merged into the instance context. */
    TASK,
    /** Executed by a worker; returns a boolean that selects {@code next} or {@code altNext}. */
    PREDICATE,
    /** Server-side timer. No worker involvement. */
    SLEEP,
    /** Fan-out: spawns one token per branch. */
    FORK,
    /** Fan-in: continues once every sibling token has arrived. */
    JOIN,
    /**
     * Waits for an external actor (a human, or another system) to complete it. No worker is
     * held. Completed out of band via the control API; an optional deadline routes to
     * {@code altNext} (escalation) or fails the instance if none is set.
     */
    USER_TASK,
    /** Terminal. */
    END
}
