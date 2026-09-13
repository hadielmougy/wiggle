package com.wiggle.client.flow;

import com.wiggle.client.dsl.Blueprint;
import com.wiggle.core.RetryPolicy;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Entry point to the future-shaped workflow API: a workflow written as a chain of method references
 * to its handler methods, compiled to the same {@link Blueprint} that
 * {@link com.wiggle.client.dsl.Workflow Workflow.define(...)} produces.
 *
 * <pre>{@code
 * OrderHandlers h = new OrderHandlers();
 *
 * Blueprint order = Wiggle.define("order-fulfilment", Order.class, f -> {
 *     var validated = f.thenApply(h::validate).thenFilter(h::inStock);
 *
 *     var payment  = validated.thenApply(h::charge).named("payment");
 *     var shipping = validated.thenApply(h::reserve).thenApply(h::label).named("shipping");
 *
 *     return Wiggle.allOf(payment, shipping)
 *                  .combine(h::settle)
 *                  .thenAccept(h::notifyCustomer);
 * });
 *
 * worker.register(order).handlers(h).start();
 * }</pre>
 *
 * <p><b>What this is.</b> A typed front-end over the existing topology DSL, nothing more. The body
 * runs exactly once, here, recording steps; what reaches the server is the same pure topology as
 * before, node for node, and the same content hash. Handlers are still bound by name on the worker;
 * the method references only supply those names in a form the compiler checks and a rename refactor
 * follows. Both APIs can be used in one codebase, and for one workflow they produce identical
 * definitions.
 *
 * <p><b>What it is not.</b> The chain is not executing and the handles are not futures over running
 * work: there is no {@code get()} and no {@code join()}. See {@link WiggleFuture} for what continuing
 * a future twice means, and for why {@code for} loops in a definition body unroll into nodes.
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
     * @param name  the workflow name, as registered with the server
     * @param input the context type the workflow starts from -- it anchors the chain's typing and is
     *              not otherwise used; the graph carries no context type
     * @param body  the chain, run once, here
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
        Plan.Step root = Plan.root();
        WiggleFuture<?> tail = body.apply(new WiggleFuture<>(root));
        if (tail == null) {
            throw new IllegalStateException(
                    "the body of workflow '" + name + "' returned null; it must return the future it ends on");
        }
        return Plan.compile(name, defaultRetry, root);
    }

    // ------------------------------------------------------------------ fan-out

    /**
     * Fans the flow out over futures that branched from a common point, and returns the mandatory
     * combine stage. The futures are the arms: each runs on its <em>own isolated copy</em> of the
     * context -- an arm's writes are invisible to its siblings and never touch the shared context --
     * so the combine is the only way an arm's result reaches the flow.
     *
     * <pre>{@code
     * var payment  = validated.thenApply(h::charge).named("payment");
     * var shipping = validated.thenApply(h::label).named("shipping");
     * Wiggle.allOf(payment, shipping).combine(h::settle);
     * }</pre>
     *
     * <p>Unlike {@link java.util.concurrent.CompletableFuture#allOf}, the arms are not already
     * running and the result is not {@code Void}: this records where the graph forks, and the combine
     * that follows records where it rejoins. The arms must fan out from one common step -- that step
     * is where the fork node lands. Each arm's name comes from {@link WiggleFuture#named}, or from its
     * last step; the engine keys each branch's result by that name for the combine handler's
     * {@link com.wiggle.client.worker.Arm @Arm} parameters.
     */
    public static <A, B> WiggleFuture.Fork2<A, B> allOf(WiggleFuture<A> a, WiggleFuture<B> b) {
        return new WiggleFuture.Fork2<>(Plan.fork(steps(a, b)));
    }

    /** Three-armed {@link #allOf(WiggleFuture, WiggleFuture)}. */
    public static <A, B, C> WiggleFuture.Fork3<A, B, C> allOf(WiggleFuture<A> a, WiggleFuture<B> b,
                                                              WiggleFuture<C> c) {
        return new WiggleFuture.Fork3<>(Plan.fork(steps(a, b, c)));
    }

    /**
     * {@link #allOf(WiggleFuture, WiggleFuture)} with any number of arms. Past three the combine's
     * parameters outrun what a functional interface can express, so
     * {@link WiggleFuture.ForkN#combine(String, Class)} names the combine handler instead of
     * referencing it.
     */
    public static WiggleFuture.ForkN allOf(WiggleFuture<?>... arms) {
        return new WiggleFuture.ForkN(Plan.fork(steps(arms)));
    }

    private static List<Plan.Step> steps(WiggleFuture<?>... futures) {
        List<Plan.Step> steps = new ArrayList<>(futures.length);
        for (WiggleFuture<?> future : futures) {
            if (future == null) throw new IllegalArgumentException("allOf was given a null future");
            steps.add(future.step());
        }
        return steps;
    }
}
