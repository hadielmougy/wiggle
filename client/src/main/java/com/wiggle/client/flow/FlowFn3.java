package com.wiggle.client.flow;

import java.io.Serializable;

/**
 * A 3-parameter handler method, given as a direct method reference: the combine of a
 * 3-armed fan-out, or a 2-armed one that also takes the pre-fork
 * {@link com.wiggle.client.worker.Context @Context}. The parameters are the arms' results, in
 * fork order -- a combine takes one per arm and binds them by position.
 */
@FunctionalInterface
public interface FlowFn3<A, B, C, R> extends Serializable {

    R apply(A a, B b, C c);
}
