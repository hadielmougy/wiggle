package com.wiggle.order;

import com.wiggle.client.worker.Handlers;

/**
 * The step logic for {@link OrderFulfilment}, bound on a worker by name. Each method's name matches a
 * step (case/style-insensitive, so {@code inStock} serves {@code in-stock}) and its signature defines
 * the step: an {@link Order} in and out is a task, a {@code boolean} is a gate, {@code void} is an
 * effect. The {@code merge} combine has no method here, so its branches fold with the default union.
 */
@Handlers("order-fulfilment")
public final class OrderHandlers {

    public Order validate(Order order) {
        if (order.customer() == null || order.customer().isBlank()) {
            throw new IllegalArgumentException("order has no customer");
        }
        return order.withStatus("VALIDATED").log("validated");
    }

    public boolean inStock(Order order) {
        return order.quantity() > 0;
    }

    public Order authorise(Order order) {
        return order.withPaymentRef("auth-" + order.orderId());
    }

    public Order capture(Order order) {
        return order.log("captured " + order.amount());
    }

    public Order reserveStock(Order order) {
        return order.withShipmentRef("shp-" + order.orderId());
    }

    public Order printLabel(Order order) {
        return order.withTrackingLabel("DHL-" + order.orderId().toUpperCase());
    }

    public Order notify(Order order) {
        return order.withStatus("FULFILLED").log("customer notified");
    }

    public void audit(Order order) {
        System.out.println("   [worker] " + order.orderId() + " -> " + order.status()
                + " " + order.amount() + " " + order.currency()
                + " payment=" + order.paymentRef() + " tracking=" + order.trackingLabel());
    }
}
