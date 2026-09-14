package com.wiggle.client.flow;

import com.wiggle.core.RetryPolicy;

import java.util.function.UnaryOperator;

/**
 * One arm of a {@link GraphBuilder#choose} -- a named guard plus the branch to run when it is the
 * first guard to hold. The guard's logic is a boolean-returning handler method bound on the worker
 * by {@code name}. A {@link #otherwise} arm has no guard and runs only when no earlier guard matched.
 *
 * <p>A guard is worker-dispatched like any other, so it may carry its own {@code retry} policy and
 * {@code queue}, given here on the arm itself -- a {@code choose} adds several guards at once, so
 * there is no single "one just added" to amend. An {@code otherwise} arm has no guard and so takes
 * neither.
 */
record Case(String name, boolean guarded, RetryPolicy retry, String queue,
                   UnaryOperator<GraphBuilder> body) {

    /** A guarded arm: its branch runs when the guard named {@code name} is the first case to test true. */
    public static Case when(String name, UnaryOperator<GraphBuilder> body) {
        return new Case(name, true, null, null, body);
    }

    /** {@link #when(String, UnaryOperator)} with an explicit retry policy for the guard. */
    public static Case when(String name, RetryPolicy retry, UnaryOperator<GraphBuilder> body) {
        return new Case(name, true, retry, null, body);
    }

    /** {@link #when(String, UnaryOperator)} with the guard pinned to a dedicated worker queue. */
    public static Case when(String name, String queue, UnaryOperator<GraphBuilder> body) {
        return new Case(name, true, null, queue, body);
    }

    /** {@link #when(String, UnaryOperator)} with both a retry policy and a dedicated queue. */
    public static Case when(String name, RetryPolicy retry, String queue,
                            UnaryOperator<GraphBuilder> body) {
        return new Case(name, true, retry, queue, body);
    }

    /** {@link #when(String, RetryPolicy, String, UnaryOperator)}, queue first. */
    public static Case when(String name, String queue, RetryPolicy retry,
                            UnaryOperator<GraphBuilder> body) {
        return new Case(name, true, retry, queue, body);
    }

    /** The default arm: runs when no guarded case matched. Must be the last case given to {@code choose}. */
    public static Case otherwise(String name, UnaryOperator<GraphBuilder> body) {
        return new Case(name, false, null, null, body);
    }
}