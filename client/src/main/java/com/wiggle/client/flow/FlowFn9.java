package com.wiggle.client.flow;

import java.io.Serializable;

/**
 * A 9-parameter handler method, given as a direct method reference: the combine of a
 * 9-armed fan-out, or a 8-armed one that also takes the pre-fork
 * {@link com.wiggle.client.worker.Context @Context}. The parameters are the arms' results, in
 * fork order -- a combine takes one per arm and binds them by position.
 */
@FunctionalInterface
public interface FlowFn9<A, B, C, D, E, F, G, H, I, R> extends Serializable {

    R apply(A a, B b, C c, D d, E e, F f, G g, H h, I i);
}
