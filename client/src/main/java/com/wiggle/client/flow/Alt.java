package com.wiggle.client.flow;

import com.wiggle.core.RetryPolicy;

import java.util.function.UnaryOperator;

/**
 * One arm of a {@link WiggleFlow#thenChoose choose}: a guard and the branch that runs when it is
 * the first guard to hold. Exactly one arm ever runs. An {@link #otherwise} arm has no guard and runs
 * only when none of the guarded arms matched; it must be given last.
 *
 * <p>Unlike a fan-out, a choose does not branch the context -- its arms share the one context -- so
 * its branches must leave the context type as they found it.
 *
 * <p>A guard is worker-dispatched like any other step, so it may carry its own {@code retry} policy
 * and {@code queue}. They are given here rather than amended afterwards with
 * {@link WiggleFlow#withRetry}, because a choose records several guards at once and there is no "the
 * one just recorded" to amend. An {@code otherwise} arm has no guard and so takes neither.
 *
 * @param <T> the context type the choose sits on
 */
public record Alt<T>(String name, boolean guarded, RetryPolicy retry, String queue,
                     UnaryOperator<WiggleFlow<T>> body) {

    public Alt {
        if (name == null || name.isBlank()) throw new IllegalArgumentException("a choose case needs a name");
        if (body == null) throw new IllegalArgumentException("choose case '" + name + "' has no body");
    }

    /** A guarded arm; the node name comes from the guard method the reference names. */
    public static <T> Alt<T> when(FlowGate<T> guard, UnaryOperator<WiggleFlow<T>> body) {
        return new Alt<>(StepNames.of(guard), true, null, null, body);
    }

    /** {@link #when(FlowGate, UnaryOperator)} with an explicit retry policy for the guard. */
    public static <T> Alt<T> when(FlowGate<T> guard, RetryPolicy retry, UnaryOperator<WiggleFlow<T>> body) {
        return new Alt<>(StepNames.of(guard), true, retry, null, body);
    }

    /** {@link #when(FlowGate, UnaryOperator)} with the guard pinned to a dedicated worker queue. */
    public static <T> Alt<T> when(FlowGate<T> guard, String queue, UnaryOperator<WiggleFlow<T>> body) {
        return new Alt<>(StepNames.of(guard), true, null, queue, body);
    }

    /** {@link #when(FlowGate, UnaryOperator)} with both a retry policy and a dedicated queue. */
    public static <T> Alt<T> when(FlowGate<T> guard, RetryPolicy retry, String queue,
                                  UnaryOperator<WiggleFlow<T>> body) {
        return new Alt<>(StepNames.of(guard), true, retry, queue, body);
    }

    /** {@link #when(FlowGate, RetryPolicy, String, UnaryOperator)}, queue first. */
    public static <T> Alt<T> when(FlowGate<T> guard, String queue, RetryPolicy retry,
                                  UnaryOperator<WiggleFlow<T>> body) {
        return new Alt<>(StepNames.of(guard), true, retry, queue, body);
    }

    /** The default arm -- no guard, so nothing to reference; it is named for the console diagram only. */
    public static <T> Alt<T> otherwise(String name, UnaryOperator<WiggleFlow<T>> body) {
        return new Alt<>(name, false, null, null, body);
    }
}