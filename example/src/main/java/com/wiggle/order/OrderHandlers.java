package com.wiggle.order;

import com.wiggle.client.worker.Context;
import com.wiggle.client.worker.ForFlow;

/**
 * The step logic for {@link OrderFulfilment}, bound on a worker by name. Each method's name matches a
 * step (case/style-insensitive, so {@code inStock} serves {@code in-stock}) and its signature defines
 * the step: an {@link Order} in and out is a task, a {@code boolean} is a gate, {@code void} is an
 * effect. The {@code merge} method is the combine: it receives each branch's result and the pre-fork
 * context, and returns the complete post-join order (combines are always explicit). Its two arm
 * parameters bind by position, in fork order.
 *
 * <p>It implements {@link OrderSteps}, the contract {@link OrderFulfilment} names the topology
 * through, so the compiler checks that every step the spec declares exists here with the right
 * signature. The {@code @Override}s are the point: drop a step and the build fails, rather than the
 * worker failing to bind at startup.
 */
@ForFlow("order-fulfilment")
public final class OrderHandlers implements OrderSteps {

    @Override
    public Order validate(Order order) {
        if (order.customer() == null || order.customer().isBlank()) {
            throw new IllegalArgumentException("order has no customer");
        }
        return order.withStatus("VALIDATED").log("validated");
    }

    @Override
    public boolean inStock(Order order) {
        return order.quantity() > 0;
    }

    @Override
    public Order authorise(Order order) {
        return order.withPaymentRef("auth-" + order.orderId());
    }

    @Override
    public Order capture(Order order) {
        return order.log("captured " + order.amount());
    }

    @Override
    public Order reserveStock(Order order) {
        return order.withShipmentRef("shp-" + order.orderId());
    }

    @Override
    public Order printLabel(Order order) {
        return order.withTrackingLabel("DHL-" + order.orderId().toUpperCase());
    }

    /** The combine: fold what each branch produced onto the pre-fork order — the return is complete. */
    @Override
    public Order merge(@Context Order base, Order payment, Order shipping) {
        return base.withPaymentRef(payment.paymentRef())
                .withShipmentRef(shipping.shipmentRef())
                .withTrackingLabel(shipping.trackingLabel());
    }

    @Override
    public Order notify(Order order) {
        return order.withStatus("FULFILLED").log("customer notified");
    }

    @Override
    public void audit(Order order) {
        System.out.println("   [worker] " + order.orderId() + " -> " + order.status()
                + " " + order.amount() + " " + order.currency()
                + " payment=" + order.paymentRef() + " tracking=" + order.trackingLabel());
    }
}
