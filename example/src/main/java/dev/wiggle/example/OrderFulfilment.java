package dev.wiggle.example;

import dev.wiggle.client.dsl.Blueprint;
import dev.wiggle.client.dsl.Branch;
import dev.wiggle.client.dsl.Workflow;
import dev.wiggle.core.ContextCodec;
import dev.wiggle.core.RetryPolicy;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The workflow definition and its implementation, in one place. The chain compiles to
 * a graph the server can drive, while the lambdas stay behind on the worker.
 */
public final class OrderFulfilment {

    /** Stands in for a flaky downstream: the first two attempts per order blow up. */
    private static final Map<String, Integer> GATEWAY_ATTEMPTS = new ConcurrentHashMap<>();

    private OrderFulfilment() {}

    public static Blueprint<Order> blueprint() {
        return Workflow.define("order-fulfilment", ContextCodec.records(Order.class))

                .map("validate", order -> {
                    if (order.customer() == null || order.customer().isBlank()) {
                        throw new IllegalArgumentException("order has no customer");
                    }
                    return order.withStatus("VALIDATED").log("validated");
                })

                // A false predicate ends the instance successfully, like an empty stream.
                .filter("in-stock", order -> order.quantity() > 0)

                .fork(
                        Branch.of("payment", s -> s
                                .map("authorise", order -> {
                                    int attempt = GATEWAY_ATTEMPTS.merge(order.orderId(), 1, Integer::sum);
                                    if (attempt <= 2) {
                                        throw new IllegalStateException(
                                                "payment gateway timeout (attempt " + attempt + ")");
                                    }
                                    return order.withPaymentRef("auth-" + order.orderId());
                                })
                                .retry(RetryPolicy.exponential(5, Duration.ofMillis(100)))
                                .map("capture", order -> order.log("captured " + order.amount()))),

                        Branch.of("shipping", s -> s
                                .map("reserve-stock", order -> order.withShipmentRef("shp-" + order.orderId()))
                                // A server-side timer: no worker is held while we wait.
                                .sleep("await-warehouse", Duration.ofMillis(300))
                                .map("print-label", order ->
                                        order.withTrackingLabel("DHL-" + order.orderId().toUpperCase())))
                )

                .map("notify", order -> order.withStatus("FULFILLED").log("customer notified"))

                .peek("audit", order -> System.out.println(
                        "   [worker] " + order.orderId() + " -> " + order.status()
                                + " payment=" + order.paymentRef() + " tracking=" + order.trackingLabel()))

                .build();
    }
}
