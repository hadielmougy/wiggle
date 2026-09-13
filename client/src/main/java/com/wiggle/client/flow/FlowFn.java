package com.wiggle.client.flow;

import java.io.Serializable;

/**
 * A task step, given as a <em>method reference to a handler method</em>: {@code account::withdraw}.
 * The reference is never invoked while the flow is defined -- it is a name the compiler type-checks.
 * {@link com.wiggle.client.flow.Wiggle Wiggle} reads the referenced method's name off the reference
 * and emits a step node with it; the worker binds that node back to the same method at run time.
 *
 * <p>Extending {@link Serializable} is what makes the name readable: javac gives a serializable
 * lambda a {@code writeReplace} yielding a {@link java.lang.invoke.SerializedLambda} that carries the
 * implementation method's name and descriptor. Only a <em>direct</em> method reference works --
 * {@code o -> account.withdraw(o)} compiles to a synthetic {@code lambda$...} method and is rejected.
 *
 * <p>The parameter is the step's input: the persisted context decoded into {@code A}. It is not an
 * argument the caller supplies -- a flow never passes values into a step, the engine does.
 *
 * @param <A> the context type the step consumes
 * @param <B> the context type it produces
 */
@FunctionalInterface
public interface FlowFn<A, B> extends Serializable {

    B apply(A input);
}