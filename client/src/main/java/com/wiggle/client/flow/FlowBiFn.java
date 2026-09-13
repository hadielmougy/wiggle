package com.wiggle.client.flow;

import java.io.Serializable;

/**
 * A two-parameter handler method, given as a direct method reference. Used for the combine of a
 * two-armed {@link WiggleFlow.Fork2} -- where the parameters are the arms' results, in fork order, each
 * {@link com.wiggle.client.worker.Arm @Arm}-annotated on the handler -- and for a {@code forEach}
 * combine taking the {@link com.wiggle.client.worker.Context @Context} plus the collected items.
 */
@FunctionalInterface
public interface FlowBiFn<A, B, R> extends Serializable {

    R apply(A a, B b);
}