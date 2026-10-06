package com.wiggle.client.flow;

import java.io.Serializable;

/**
 * A 4-parameter handler method, given as a direct method reference -- typically a combine,
 * whose parameters the worker finds by type, in any order (see {@link Combines}).
 */
@FunctionalInterface
public interface FlowFn4<A, B, C, D, R> extends Serializable {

    R apply(A a, B b, C c, D d);
}
