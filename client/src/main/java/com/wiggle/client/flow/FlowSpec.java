package com.wiggle.client.flow;

import com.wiggle.core.RetryPolicy;
import com.wiggle.core.WorkflowDefinition;

import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * The output of {@code build()}: an immutable workflow topology (nodes, edges, kinds, queues, retry)
 * to register with the server. It carries no step logic and no context type -- the graph is the
 * whole artifact. Step implementations live in {@link com.wiggle.client.worker.Handlers @Handlers}
 * classes bound on a worker and matched to the graph by name.
 */
public record FlowSpec(WorkflowDefinition definition) {

    public String name() { return definition.name(); }

    public int version() { return definition.version(); }

    public Set<String> queues() { return definition.queues(); }


    /**
     * Defines a workflow whose first step takes {@code input}.
     *
     * @param name  the workflow name, as registered with the server
     * @param input the context type the workflow starts from -- it anchors the chain's typing and is
     *              not otherwise used; the graph carries no context type
     * @param body  the chain, run once, here
     */
    public static <T> FlowSpec define(String name, Class<T> input,
                                      Function<WiggleFlow<T>, WiggleFlow<?>> body) {
        return define(name, null, input, body);
    }

    /**
     * Defines a workflow whose steps are declared by {@code contract} -- an interface naming each
     * step and its signature, with no implementation:
     *
     * <pre>{@code
     * interface OrderSteps {
     *     Order   validate(Order o);
     *     boolean inStock(Order o);
     *     Payment charge(Order o);
     * }
     *
     * FlowSpec order = FlowSpec.define("order-fulfilment", Order.class, OrderSteps.class, (f, s) -> f
     *         .thenApply(s::validate)
     *         .thenFilter(s::inStock));
     * }</pre>
     *
     * <p>{@code s} is an inert stand-in: the body only <em>names</em> steps through it, and calling a
     * method on it throws. That is deliberate. A spec never runs a step -- it records the step's name,
     * and a worker supplies the code by matching that name to a method on its {@code @Handlers}
     * object. Naming the steps on an interface says exactly that, where a reference to a concrete
     * class reads as though the spec will call it.
     *
     * <p>On the worker, a handler <em>should</em> implement the same interface -- then the compiler
     * guarantees every step's name and signature match what the spec declared, and the two cannot
     * drift. It is not required: binding is by name, as it always was, so a handler that merely
     * happens to match still works.
     */
    public static <T, H> FlowSpec define(String name, Class<T> input, Class<H> contract,
                                         BiFunction<WiggleFlow<T>, H, WiggleFlow<?>> body) {
        return define(name, null, input, contract, body);
    }

    /** {@link #define(String, Class, Class, BiFunction)} with an explicit default retry policy. */
    public static <T, H> FlowSpec define(String name, RetryPolicy defaultRetry, Class<T> input,
                                         Class<H> contract,
                                         BiFunction<WiggleFlow<T>, H, WiggleFlow<?>> body) {
        if (body == null) throw new IllegalArgumentException("workflow '" + name + "' has no body");
        H steps = Steps.of(contract);
        return define(name, defaultRetry, input, f -> body.apply(f, steps));
    }

    /**
     * {@link #define(String, Class, Function)} with an explicit default retry policy for every step
     * that does not name its own.
     */
    public static <T> FlowSpec define(String name, RetryPolicy defaultRetry, Class<T> input,
                                      Function<WiggleFlow<T>, WiggleFlow<?>> body) {
        if (body == null) throw new IllegalArgumentException("workflow '" + name + "' has no body");
        Plan.Step root = Plan.root();
        WiggleFlow<?> tail = body.apply(new WiggleFlow<>(root));
        if (tail == null) {
            throw new IllegalStateException(
                    "the body of workflow '" + name + "' returned null; it must return the handle it ends on");
        }
        return Plan.compile(name, defaultRetry, root);
    }
}
