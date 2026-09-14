package com.wiggle.client.flow;

import java.io.Serializable;

/**
 * A step given as a method reference to a <em>zero-argument factory</em> that produces the activity
 * which runs it: {@code orders::reserve}. Like every other reference in a flow it is never invoked
 * here -- it is a name the compiler type-checks, and the declared return type is what says whether
 * the step carries an undo.
 *
 * <p>This is the shape a typed activity takes on the worker, so a contract that names steps this way
 * mirrors the handler exactly: the same zero-argument signature, the same return type. See
 * {@link WiggleFlow#thenActivity} and
 * {@link com.wiggle.client.worker.CompensableActivity CompensableActivity}.
 *
 * @param <A> the activity type the factory declares -- its type argument is the step's context type
 */
@FunctionalInterface
public interface FlowFactory<A> extends Serializable {

    A get();
}
