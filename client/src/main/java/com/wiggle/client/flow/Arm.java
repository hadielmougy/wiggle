package com.wiggle.client.flow;

import java.util.function.Function;

/**
 * One arm of a {@link WiggleFuture#thenFork fork}: a name and the chain to run on it. The arm starts
 * from a handle on the pre-fork context and ends wherever its last step leaves it -- that end type is
 * what the combine's matching parameter receives, which is how a fork stays type-checked across the
 * fan-out.
 *
 * <p>The name is not decoration: it is how the engine keys this arm's result for the combine
 * handler's {@link com.wiggle.client.worker.Arm @Arm} parameter. An arm runs on its own isolated copy
 * of the context, so nothing it writes is visible anywhere else until the combine.
 *
 * @param <T> the context type the arm starts from
 * @param <R> the context type the arm ends on
 */
public record Arm<T, R>(String name, Function<WiggleFuture<T>, WiggleFuture<R>> body) {

    public Arm {
        if (name == null || name.isBlank()) throw new IllegalArgumentException("a fork arm needs a name");
        if (body == null) throw new IllegalArgumentException("fork arm '" + name + "' has no body");
    }

    public static <T, R> Arm<T, R> of(String name, Function<WiggleFuture<T>, WiggleFuture<R>> body) {
        return new Arm<>(name, body);
    }
}