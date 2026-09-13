package com.wiggle.client.flow;

import java.io.Serializable;

/**
 * A 10-parameter handler method, given as a direct method reference: the combine of a
 * 10-armed fan-out, or a 9-armed one that also takes the pre-fork
 * {@link com.wiggle.client.worker.Context @Context}. The parameters are the arms' results in
 * fork order, each optionally {@link com.wiggle.client.worker.Arm @Arm}-annotated.
 */
@FunctionalInterface
public interface FlowFn10<A, B, C, D, E, F, G, H, I, J, R> extends Serializable {

    R apply(A a, B b, C c, D d, E e, F f, G g, H h, I i, J j);
}
