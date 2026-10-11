package com.wiggle.core.criteria;

/**
 * What a comparison can test: a closed set of task properties. On the wire a field travels by
 * {@link #name()}, and the server maps each one to a fixed column.
 */
public enum Field {
    WORKFLOW(Type.TEXT),
    VERSION(Type.INTEGER),
    QUEUE(Type.TEXT),
    /** The step's name as authored; compared by equality only. */
    STEP(Type.TEXT),
    NODE_ID(Type.TEXT),
    /** {@code TASK} or {@code PREDICATE}. */
    KIND(Type.TEXT),
    /** The attempt the worker would see: 1 on the first run. */
    ATTEMPT(Type.INTEGER),
    CORRELATION_ID(Type.TEXT),
    /** Epoch millis of the instance's creation. */
    INSTANCE_CREATED_AT(Type.INTEGER);

    /** How a field's literals are typed: TEXT takes {@link String}, INTEGER takes {@link Long}. */
    public enum Type { TEXT, INTEGER }

    private final Type type;

    Field(Type type) {
        this.type = type;
    }

    public Type type() {
        return type;
    }
}
