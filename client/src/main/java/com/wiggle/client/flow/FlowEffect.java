package com.wiggle.client.flow;

import java.io.Serializable;

/**
 * An effect step -- a handler method that returns {@code void}, leaving the context unchanged. Given
 * as a direct method reference, exactly like {@link FlowFn}.
 *
 * @param <A> the context type the effect observes
 */
@FunctionalInterface
public interface FlowEffect<A> extends Serializable {

    void accept(A input);
}