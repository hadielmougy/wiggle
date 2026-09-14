package com.wiggle.client.flow;

import java.io.Serializable;

/**
 * A 2-parameter handler method, given as a direct method reference: the combine of a
 * 2-armed fan-out, or a 1-armed one that also takes the pre-fork
 * {@link com.wiggle.client.worker.Context @Context}. The parameters are the arms' results, in
 * fork order -- a combine takes one per arm and binds them by position.
 */
@FunctionalInterface
public interface FlowFn2<A, B, R> extends Serializable {

    R apply(A a, B b);
}
