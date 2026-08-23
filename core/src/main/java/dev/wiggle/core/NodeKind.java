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
    /**
     * Runtime fan-out: spawns one token per element of a list read from the context at
     * {@code itemsKey}, each running the same branch template with its element injected
     * under {@code itemKey} (and its position under {@code itemKey + "Index"}).
     */
    DYN_FORK,
    /** Fan-in: continues once every sibling token has arrived. */
    JOIN,
    /**
     * Waits for a named signal from an external actor (a human, or another system). No worker
     * is held. Delivered via the control API by (instance, signal name); an optional deadline
     * routes to {@code altNext} (escalation) or fails the instance if none is set.
     */
    SIGNAL,
    /**
     * Starts an instance of another workflow with this instance's context as input, then waits;
     * the child's final context merges back on completion, and a failed or cancelled child fails
     * the parent.
     */
    SUB_WORKFLOW,
    /** Terminal. */
    END
}
