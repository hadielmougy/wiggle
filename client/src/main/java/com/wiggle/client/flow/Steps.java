package com.wiggle.client.flow;

import java.lang.reflect.Proxy;

/**
 * The stand-in a spec names its steps through. {@link Wiggle#define(String, Class, Class,
 * java.util.function.BiFunction)} hands one to the definition body, and every {@code thenApply(s::...)}
 * in that body is a reference to a method of the <em>interface</em> -- a contract, not an
 * implementation.
 *
 * <p>That distinction is the whole point. A spec never runs a step: it records the step's name, and a
 * worker supplies the code by matching that name to a method on its {@code @ForFlow} object. A
 * reference to a concrete class therefore names code the spec will never call, which reads as though
 * it will -- and if the worker binds a different object, the referenced method is silently not the one
 * that runs. An interface cannot mislead that way, because there is nothing behind it to run.
 *
 * <p>So the stand-in is deliberately inert: calling any method on it throws. Nothing is ever invoked
 * on it -- the reference is read, never called -- and if some code does call one, that is a bug worth
 * hearing about rather than a silent no-op.
 */
final class Steps {

    private Steps() {}

    @SuppressWarnings("unchecked")
    static <H> H of(Class<H> contract) {
        if (contract == null || !contract.isInterface()) {
            throw new IllegalArgumentException(
                    "a workflow's steps must be declared as an interface, not "
                    + (contract == null ? "null" : contract.getName())
                    + ": the spec names the steps, and a worker supplies the code that runs them");
        }
        return (H) Proxy.newProxyInstance(
                contract.getClassLoader(), new Class<?>[] {contract},
                (proxy, method, args) -> {
                    throw new IllegalStateException(
                            "'" + method.getName() + "' was invoked on the step contract "
                            + contract.getSimpleName() + ". This object only exists so a definition can"
                            + " name its steps; the implementation runs on a worker, bound by name.");
                });
    }
}