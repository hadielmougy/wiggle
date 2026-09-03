package com.wiggle.client.dsl;

import com.wiggle.core.RetryPolicy;

/**
 * Entry point to the workflow DSL.
 *
 * <p>The workflow itself carries no context type. Each step is generic like {@code Stream.map}: it
 * takes an input type and returns a possibly different one, declaring its input {@code Class} so the
 * engine can rebuild it from the JSON persisted between steps.
 *
 * <pre>{@code
 * Blueprint bp = Workflow.define("order-fulfilment")
 *         .step("validate", Order.class,  o -> o.withStatus("VALIDATED"))   // Order  -> Order
 *         .gate("in-stock", Order.class,  o -> o.quantity() > 0)
 *         .step("price",    Order.class,  o -> new Priced(o, price(o)))     // Order  -> Priced
 *         .step("ship",     Priced.class, p -> new Shipment(p))             // Priced -> Shipment
 *         .build();
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
