package com.wiggle.client.flow;

import java.io.Serializable;

/**
 * A 8-parameter handler method, given as a direct method reference: the combine of a
 * 8-armed fan-out, or a 7-armed one that also takes the pre-fork
 * {@link com.wiggle.client.worker.Context @Context}. The parameters are the arms' results in
 * fork order, each optionally {@link com.wiggle.client.worker.Arm @Arm}-annotated.
 */
@FunctionalInterface
public interface FlowFn8<A, B, C, D, E, F, G, H, R> extends Serializable {

    R apply(A a, B b, C c, D d, E e, F f, G g, H h);
}
