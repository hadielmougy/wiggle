package com.wiggle.docs;

import com.wiggle.client.worker.Context;
import com.wiggle.client.worker.ForFlow;
import com.wiggle.docs.ForkJoinSnippet.Order;
import com.wiggle.docs.ForkJoinSnippet.OrderSteps;

/**
 * The handler half of <a href="https://wiggle.sh/patterns/fork-join/">wiggle.sh/patterns/fork-join</a>.
 * Top-level rather than nested so the page publishes {@code class}, not {@code static class} -- see
 * {@link BookingHandlers} for the reasoning.
 */
// docs:begin handlers
@ForFlow("order-fulfilment")
class OrderHandlers implements OrderSteps {      // the same contract the spec named
    // docs:skip
    private Gateway gateway;
    private Wms wms;
    private Courier courier;

    interface Gateway { String auth(Order o); }
    interface Wms { String reserve(Order o); }
    interface Courier { String label(Order o); }
    // docs:resume

    public Order   validate(Order o)     { return o.withStatus("VALIDATED"); }
    public boolean inStock(Order o)      { return o.quantity() > 0; }
    public Order   authorise(Order o)    { return o.withPaymentRef(gateway.auth(o)); }
    public Order   capture(Order o)      { return o.log("captured"); }
    public Order   reserveStock(Order o) { return o.withShipmentRef(wms.reserve(o)); }
    public Order   printLabel(Order o)   { return o.withTrackingLabel(courier.label(o)); }

    // One parameter per arm, in fork order: each is that branch's final context. The pre-fork
    // base arrives via @Context (or ambiently via Step.base()). The return is the COMPLETE
    // post-join context.
    public Order merge(@Context Order base, Order payment, Order shipping) {
        return base.withPaymentRef(payment.paymentRef())
                   .withShipmentRef(shipping.shipmentRef())
                   .withTrackingLabel(shipping.trackingLabel());
    }

    public Order notify(Order o)         { return o.withStatus("FULFILLED"); }
}
// docs:end handlers
