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
    /** Terminal. */
    END
}
