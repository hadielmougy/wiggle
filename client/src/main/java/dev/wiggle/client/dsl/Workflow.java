package dev.wiggle.client.dsl;

import dev.wiggle.core.ContextCodec;
import dev.wiggle.core.RetryPolicy;

import java.util.Map;

/**
 * Entry point to the workflow DSL.
 *
 * <pre>{@code
 * Blueprint<Order> bp = Workflow.define("order-fulfilment", ContextCodec.records(Order.class))
 *         .map("validate",  o -> o.withStatus("VALIDATED"))
 *         .filter("in-stock", o -> o.quantity() > 0)
 *         .fork(Branch.of("payment",  s -> s.map("charge", Payments::charge)),
 *               Branch.of("shipping", s -> s.map("reserve", Stock::reserve)
 *                                            .sleep(Duration.ofSeconds(2))
 *                                            .map("label", Labels::print)))
 *         .map("notify", Notifier::send)
 *         .build();
 * }</pre>
 */
public final class Workflow {

    private Workflow() {}

    public static <T> WorkflowStream<T> define(String name, ContextCodec<T> codec) {
        return define(name, codec, RetryPolicy.exponential(3, java.time.Duration.ofMillis(500)));
    }

    public static <T> WorkflowStream<T> define(String name, ContextCodec<T> codec, RetryPolicy defaultRetry) {
        Pipeline<T> pipeline = new Pipeline<>(name, codec, defaultRetry);
        return WorkflowStream.root(pipeline);
    }

    /** Convenience for contexts that are plain JSON documents. */
    public static WorkflowStream<Map<String, Object>> defineJson(String name) {
        return define(name, ContextCodec.jsonMap());
    }
}
