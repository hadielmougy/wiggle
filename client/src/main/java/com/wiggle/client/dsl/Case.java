package com.wiggle.client.dsl;

import java.util.function.UnaryOperator;

/**
 * One arm of a {@link WorkflowBuilder#choose} -- a named guard plus the branch to run when it is the
 * first guard to hold. The guard's logic is a boolean-returning handler method bound on the worker
 * by {@code name}. A {@link #otherwise} arm has no guard and runs only when no earlier guard matched.
 */
public record Case(String name, boolean guarded, UnaryOperator<WorkflowBuilder> body) {

    /** A guarded arm: its branch runs when the guard named {@code name} is the first case to test true. */
    public static Case when(String name, UnaryOperator<WorkflowBuilder> body) {
        return new Case(name, true, body);
    }

    /** The default arm: runs when no guarded case matched. Must be the last case given to {@code choose}. */
    public static Case otherwise(String name, UnaryOperator<WorkflowBuilder> body) {
        return new Case(name, false, body);
    }
}
