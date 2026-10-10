package com.wiggle.client.flow;

import java.io.Serializable;

/**
 * A 9-parameter handler method, given as a direct method reference -- typically a combine,
 * whose parameters the worker finds by type, in any order (see {@link Combines}).
 */
@FunctionalInterface
public interface FlowFn9<A, B, C, D, E, F, G, H, I, R> extends Serializable {

    R apply(A a, B b, C c, D d, E e, F f, G g, H h, I i);
}
