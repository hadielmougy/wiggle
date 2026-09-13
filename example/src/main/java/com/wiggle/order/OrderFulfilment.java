package com.wiggle.order;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.flow.Wiggle;
import com.wiggle.core.ExecutionMode;
import com.wiggle.core.RetryPolicy;

import java.time.Duration;

/**
 * The workflow <em>topology</em>: named steps and how they fork and rejoin. It compiles to a graph
 * the server drives; the step logic lives in {@link OrderHandlers}, bound on the worker by name.
 *
 * <p>Written with the future-shaped API in {@code com.wiggle.client.flow}: each step is a method
 * reference to the handler that implements it, so the compiler checks that every step consumes what
 * the one before it produced, and the node names come from the methods rather than from strings
 * typed twice. Nothing runs here -- the chain is walked once, at definition time, and compiles to
 * exactly the graph the equivalent {@code Workflow.define(...)} chain would (see
 * {@code FlowEquivalenceTest}). The same {@link OrderHandlers} instance can serve the worker.
 */
public final class OrderFulfilment {

    private OrderFulfilment() {}

    /**
     * Execution mode for benchmarking, from {@code WIGGLE_EXECUTION_MODE} (default SERVER). Set it
     * identically on the worker and submitter JVMs so they compile the same version (the mode is
     * part of the content hash).
     */
    private static ExecutionMode mode() {
        String v = System.getenv("WIGGLE_EXECUTION_MODE");
        return v == null || v.isBlank() ? ExecutionMode.SERVER : ExecutionMode.valueOf(v.trim());
    }

    public static FlowSpec flowSpec() {
        OrderHandlers h = new OrderHandlers();
        return Wiggle.define("order-fulfilment", Order.class, f -> {
            var validated = f.execution(ExecutionMode.LOCAL_ASYNC)
                    .thenApply(h::validate)
                    .thenFilter(h::inStock);

            // continuing `validated` twice is the fan-out; each arm runs on its own isolated copy
            var payment = validated
                    .thenApply(h::authorise, RetryPolicy.exponential(5, Duration.ofMillis(100)))
                    .thenApply(h::capture);
            var shipping = validated
                    .thenApply(h::reserveStock)
                    .thenApply(h::printLabel);

            // the merge needs the pre-fork order as well as both arms, so it takes the @Context;
            // its @Arm names are checked against these arms here, at definition time
            return Wiggle.allOf(payment, shipping)
                    .combineWithContext(h::merge)
                    .thenApply(h::notify)
                    .thenAccept(h::audit);
        });
    }
}
