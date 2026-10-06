package com.wiggle.client.flow;

import java.io.Serializable;

/**
 * A 2-parameter handler method, given as a direct method reference -- typically a combine,
 * whose parameters the worker finds by type, in any order (see {@link Combines}).
 */
@FunctionalInterface
public interface FlowFn2<A, B, R> extends Serializable {

    R apply(A a, B b);
}
