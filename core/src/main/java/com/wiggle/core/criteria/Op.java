package com.wiggle.core.criteria;

/** A comparison operator, with the JPA meaning of its {@link CriteriaBuilder} method. */
public enum Op {
    EQUAL(1, 1),
    NOT_EQUAL(1, 1),
    IN(1, Criteria.MAX_IN),
    LESS_THAN(1, 1),
    LESS_THAN_OR_EQUAL(1, 1),
    GREATER_THAN(1, 1),
    GREATER_THAN_OR_EQUAL(1, 1),
    BETWEEN(2, 2),
    /** {@code %} and {@code _} wildcards, {@code \} escapes; case-sensitive. */
    LIKE(1, 1),
    IS_NULL(0, 0),
    IS_NOT_NULL(0, 0);

    private final int minArgs;
    private final int maxArgs;

    Op(int minArgs, int maxArgs) {
        this.minArgs = minArgs;
        this.maxArgs = maxArgs;
    }

    public int minArgs() {
        return minArgs;
    }

    public int maxArgs() {
        return maxArgs;
    }

    /** Whether the operator orders its operands rather than testing equality. */
    public boolean ordering() {
        return this == LESS_THAN || this == LESS_THAN_OR_EQUAL || this == GREATER_THAN
                || this == GREATER_THAN_OR_EQUAL || this == BETWEEN;
    }
}
