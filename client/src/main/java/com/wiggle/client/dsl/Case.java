package com.wiggle.client.dsl;

import java.util.Map;
import java.util.function.UnaryOperator;

/**
 * One arm of a {@link WorkflowStream#choose} -- a guard plus the branch to run when it is the first
 * guard to hold. The guard, like any worker-evaluated predicate, declares its input type via
 * {@code guardType} so the engine can rebuild it from the persisted JSON. A {@code null} guard marks
 * the default ({@link #otherwise}) arm, which runs only when no earlier guard matched.
 */
public record Case(String name, Class<?> guardType, Predicate<?> guard, UnaryOperator<WorkflowStream> body) {

    /** A guarded arm: its branch runs when {@code guard} is the first case to test true. */
    public static <I> Case when(String name, Class<I> guardType, Predicate<I> guard, UnaryOperator<WorkflowStream> body) {
        if (guard == null) throw new IllegalArgumentException("when() needs a guard; use otherwise() for the default");
        return new Case(name, guardType, guard, body);
    }

    /** A guarded arm whose guard reads the raw JSON context (a {@code Map}). */
    public static Case when(String name, Predicate<Map<String, Object>> guard, UnaryOperator<WorkflowStream> body) {
        return when(name, null, guard, body);
    }

    /** The default arm: runs when no guarded case matched. Must be the last case given to {@code choose}. */
    public static Case otherwise(String name, UnaryOperator<WorkflowStream> body) {
        return new Case(name, null, null, body);
    }
}
