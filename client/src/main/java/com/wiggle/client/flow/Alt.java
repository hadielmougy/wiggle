package com.wiggle.client.flow;

import java.util.function.UnaryOperator;

/**
 * One arm of a {@link WiggleFuture#thenChoose choose}: a guard and the branch that runs when it is
 * the first guard to hold. Exactly one arm ever runs. An {@link #otherwise} arm has no guard and runs
 * only when none of the guarded arms matched; it must be given last.
 *
 * <p>Unlike a fork, a choose does not fan out -- its arms share the one context -- so its branches
 * must leave the context type as they found it.
 *
 * @param <T> the context type the choose sits on
 */
public record Alt<T>(String name, boolean guarded, UnaryOperator<WiggleFuture<T>> body) {

    public Alt {
        if (name == null || name.isBlank()) throw new IllegalArgumentException("a choose case needs a name");
        if (body == null) throw new IllegalArgumentException("choose case '" + name + "' has no body");
    }

    /** A guarded arm; the node name comes from the guard method the reference names. */
    public static <T> Alt<T> when(FlowGate<T> guard, UnaryOperator<WiggleFuture<T>> body) {
        return new Alt<>(StepNames.of(guard), true, body);
    }

    /** The default arm -- no guard, so nothing to reference; it is named for the console diagram only. */
    public static <T> Alt<T> otherwise(String name, UnaryOperator<WiggleFuture<T>> body) {
        return new Alt<>(name, false, body);
    }
}