package com.wiggle.client.dsl;

import com.wiggle.core.RetryPolicy;

/**
 * Entry point to the workflow DSL. A workflow is defined as a pure <em>topology</em> -- named steps
 * and how they chain, branch, and rejoin -- with no step logic and no context type. The
 * implementations are bound separately on a worker via a
 * {@link com.wiggle.client.worker.Handlers @Handlers} class, where each method's name matches a step
 * and its signature defines the types (input decoded from JSON, output encoded back; a method may
 * return a different type than it takes, like {@code Stream.map}).
 *
 * <pre>{@code
 * // topology
 * Blueprint order = Workflow.define("order-fulfilment")
 *         .step("validate").gate("in-stock")
 *         .fork(Branch.of("payment",  s -> s.step("charge")),
 *               Branch.of("shipping", s -> s.step("reserve").sleep(Duration.ofSeconds(2)).step("label")))
 *         .combine("settle").step("notify")
 *         .build();
 *
 * // logic
 * @Handlers("order-fulfilment")
 * class OrderHandlers {
 *     Order   validate(Order o)  { return o.withStatus("VALIDATED"); }
 *     boolean inStock(Order o)   { return o.quantity() > 0; }
 *     Order   charge(Order o)    { ... }
 *     Order   settle(@Arm("payment") Order pay, @Arm("shipping") Order ship) { ... }
 * }
 * }</pre>
 */
public final class Workflow {

    private Workflow() {}

    public static WorkflowStream define(String name) {
        return define(name, RetryPolicy.exponential(3, java.time.Duration.ofMillis(500)));
    }

    public static WorkflowStream define(String name, RetryPolicy defaultRetry) {
        return WorkflowStream.root(new Pipeline(name, defaultRetry));
    }
}
