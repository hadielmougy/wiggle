package com.wiggle.client.flow;

import java.io.Serializable;

/**
 * A guard -- a handler method returning {@code boolean}. Given as a direct method reference, exactly
 * like {@link FlowFn}. Used for gates, {@code choose} cases and {@code repeatWhile} conditions.
 *
 * @param <A> the context type the guard tests
 */
@FunctionalInterface
public interface FlowGate<A> extends Serializable {

    boolean test(A input);
}