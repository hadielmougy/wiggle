package dev.wiggle.order;

import dev.wiggle.client.dsl.Blueprint;
import dev.wiggle.client.dsl.Branch;
import dev.wiggle.client.dsl.Workflow;
import dev.wiggle.client.worker.Step;
import dev.wiggle.core.ContextCodec;
import dev.wiggle.core.ExecutionMode;
import dev.wiggle.core.RetryPolicy;

import java.time.Duration;

/**
 * The workflow definition and its implementation, in one place. The chain compiles to
 * a graph the server can drive, while the lambdas stay behind on the worker.
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

    public static Blueprint<Order> blueprint() {
        return Workflow.define("order-fulfilment", ContextCodec.records(Order.class)).execution(ExecutionMode.LOCAL_ASYNC)

                .step("validate", order -> {
                    if (order.customer() == null || order.customer().isBlank()) {
                        throw new IllegalArgumentException("order has no customer");
                    }
                    return order.withStatus("VALIDATED").log("validated");
                })

                // A false guard ends the instance successfully, like an empty stream.
                .gate("in-stock", order -> order.quantity() > 0)

                .fork(
                        Branch.of("payment", s -> s
                                // Stands in for a flaky downstream: the first two attempts blow up.
                                // Step.attempt() is the engine's global count, so retries converge no
                                // matter which worker picks up each try. The retry policy is passed inline.
                                .step("authorise", order -> {
                                    /*
                                    if (Step.attempt() <= 2) {
                                        throw new IllegalStateException(
                                                "payment gateway timeout (attempt " + Step.attempt() + ")");
                                    }
                                     */
                                    return order.withPaymentRef("auth-" + order.orderId());
                                }, RetryPolicy.exponential(5, Duration.ofMillis(100)))
                                .step("capture", order -> order.log("captured " + order.amount()))),

                        Branch.of("shipping", s -> s
                                .step("reserve-stock", order -> order.withShipmentRef("shp-" + order.orderId()))
                                // A server-side timer: no worker is held while we wait.
                                .sleep("await-warehouse", Duration.ofMillis(100))
                                .step("print-label", order ->
                                        order.withTrackingLabel("DHL-" + order.orderId().toUpperCase())))
                )

                .step("notify", order -> order.withStatus("FULFILLED").log("customer notified"))

                .effect("audit", order -> System.out.println(
                        "   [worker] " + order.orderId() + " -> " + order.status()
                                + " payment=" + order.paymentRef() + " tracking=" + order.trackingLabel()))

                .build();
    }
}
