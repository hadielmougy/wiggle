package com.wiggle.client.flow;

import java.io.Serializable;

/**
 * A three-parameter handler method, given as a direct method reference: the combine of a three-armed
 * {@link WiggleFuture.Fork3}. The parameters are the arms' results in fork order, each
 * {@link com.wiggle.client.worker.Arm @Arm}-annotated on the handler.
 */
@FunctionalInterface
public interface FlowTriFn<A, B, C, R> extends Serializable {

    R apply(A a, B b, C c);
}