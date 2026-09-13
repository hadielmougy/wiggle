package com.wiggle.client.flow;

import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.core.RetryPolicy;

import java.util.function.Function;

/**
 * Entry point to the future-shaped workflow API: a workflow written as a chain of method references
 * to its handler methods, compiled to the same {@link Blueprint} that
 * {@link com.wiggle.client.dsl.Workflow Workflow.define(...)} produces.
 *
 * <pre>{@code
 * OrderHandlers h = new OrderHandlers();
 *
 * Blueprint order = Wiggle.define("order-fulfilment", Order.class, f -> f
 *         .thenApply(h::validate)
 *         .thenFilter(h::inStock)
 *         .thenFork(Arm.of("payment",  a -> a.thenApply(h::charge)),
 *                   Arm.of("shipping", a -> a.thenApply(h::reserve).thenApply(h::label)))
 *         .combine(h::settle)
 *         .thenAccept(h::notifyCustomer));
 *
 * worker.register(order).handlers(h).start();
 * }</pre>
 *
 * <p><b>What this is.</b> A typed front-end over the existing topology DSL, nothing more. The body
 * runs exactly once, here, and every {@code then*} call appends a node -- so what reaches the server
 * is the same pure topology as before, node for node, and the same content hash. Handlers are still
 * bound by name on the worker; the method references only supply those names in a form the compiler
 * checks and a rename refactor follows. Both APIs can be used in one codebase, and for one workflow
 * they produce identical definitions.
 *
 * <p><b>What it is not.</b> The chain is not executing and the handles are not futures over running
 * work: there is no {@code get()}, no {@code join()}, and a handle cannot be continued twice. See
 * {@link WiggleFuture} for the two rules that keeps, and for why {@code for} loops in a definition
 * body unroll into nodes.
 *
 * <p>The handler object is used here only as the receiver the method references name; nothing on it
 * is invoked while the workflow is defined. It may be the very instance later given to
 * {@link com.wiggle.client.worker.Worker#handlers Worker.handlers(...)}, which is the point -- one
 * object carries the topology and the logic, still matched by name.
 */
public final class Wiggle {

    private Wiggle() {}

    /**
     * Defines a workflow whose first step takes {@code input}.
     *
     * @param name     the workflow name, as registered with the server
     * @param input    the context type the workflow starts from -- it anchors the chain's typing and
     *                 is not otherwise used; the graph carries no context type
     * @param body     the chain, run once, here
     */
    public static <T> Blueprint define(String name, Class<T> input,
                                       Function<WiggleFuture<T>, WiggleFuture<?>> body) {
        return define(name, null, input, body);
    }

    /**
     * {@link #define(String, Class, Function)} with an explicit default retry policy for every step
     * that does not name its own.
     */
    public static <T> Blueprint define(String name, RetryPolicy defaultRetry, Class<T> input,
                                       Function<WiggleFuture<T>, WiggleFuture<?>> body) {
        if (body == null) throw new IllegalArgumentException("workflow '" + name + "' has no body");
        WiggleFuture<T> start = new WiggleFuture<>(
                defaultRetry == null ? Workflow.define(name) : Workflow.define(name, defaultRetry));
        WiggleFuture<?> tail = body.apply(start);
        if (tail == null) {
            throw new IllegalStateException(
                    "the body of workflow '" + name + "' returned null; it must return the future it ends on");
        }
        return tail.stream().build();
    }
}