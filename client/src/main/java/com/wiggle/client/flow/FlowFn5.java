package com.wiggle.client.flow;

import java.io.Serializable;

/**
 * A 5-parameter handler method, given as a direct method reference -- typically a combine,
 * whose parameters the worker finds by type, in any order (see {@link Combines}).
 */
@FunctionalInterface
public interface FlowFn5<A, B, C, D, E, R> extends Serializable {

    R apply(A a, B b, C c, D d, E e);
}
