package com.wiggle.client.flow;

import java.lang.reflect.Proxy;

/**
 * The stand-in a spec names its steps through. {@link Wiggle#define(String, Class, Class,
 * java.util.function.BiFunction)} hands one to the definition body, and every {@code thenApply(s::...)}
 * there references a method of the <em>interface</em>: a spec records the step's name, and a worker
 * supplies the code by matching it to a method on its {@code @ForFlow} object.
 *
 * <p>Inert by design -- the reference is read, never called, so every method throws.
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