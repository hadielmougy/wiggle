package dev.wiggle.client.dsl;

import java.util.function.UnaryOperator;

/**
 * One arm of a {@link WorkflowStream#choose} -- a guard plus the branch to run when it is the
 * first guard to hold. A {@code null} guard marks the default ({@link #otherwise}) arm, which
 * runs only when no earlier guard matched.
 */
public record Case<T>(String name, Predicate<T> guard, UnaryOperator<WorkflowStream<T>> body) {

    /** A guarded arm: its branch runs when {@code guard} is the first case to test true. */
    public static <T> Case<T> when(String name, Predicate<T> guard, UnaryOperator<WorkflowStream<T>> body) {
        if (guard == null) throw new IllegalArgumentException("when() needs a guard; use otherwise() for the default");
        return new Case<>(name, guard, body);
    }

    /** The default arm: runs when no guarded case matched. Must be the last case given to {@code choose}. */
    public static <T> Case<T> otherwise(String name, UnaryOperator<WorkflowStream<T>> body) {
        return new Case<>(name, null, body);
    }
}
