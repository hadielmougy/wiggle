package com.wiggle.client.flow;

import java.io.Serializable;

/**
 * A 8-parameter handler method, given as a direct method reference -- typically a combine,
 * whose parameters the worker finds by type, in any order (see {@link Combines}).
 */
@FunctionalInterface
public interface FlowFn8<A, B, C, D, E, F, G, H, R> extends Serializable {

    R apply(A a, B b, C c, D d, E e, F f, G g, H h);
}
