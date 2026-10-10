package com.wiggle.client.flow;

import java.io.Serializable;

/**
 * A 3-parameter handler method, given as a direct method reference -- typically a combine,
 * whose parameters the worker finds by type, in any order (see {@link Combines}).
 */
@FunctionalInterface
public interface FlowFn3<A, B, C, R> extends Serializable {

    R apply(A a, B b, C c);
}
