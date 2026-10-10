package com.wiggle.docs.onboarding;

import com.wiggle.docs.onboarding.OnboardingSnippet.OrderSteps;
import com.wiggle.client.worker.ForFlow;
import com.wiggle.client.worker.Step;
import com.wiggle.docs.onboarding.OnboardingSnippet.Order;

/** The handler class quoted in {@code docs/onboarding.md}. */
// docs:begin handlers
@ForFlow("order-fulfilment")
class OrderHandlers implements OrderSteps {
    // docs:skip
    static String auth(Order o) { return "auth"; }
    static String reserveRef(Order o) { return "ship"; }
    static String print(Order o) { return "label"; }
    // docs:resume
    public Order   validate(Order o)  { return o.withStatus("VALIDATED"); }
    public boolean inStock(Order o)   { return o.quantity() > 0; }        // gate
    public Order   authorise(Order o) { return o.withPaymentRef(auth(o)); }
    public Order   capture(Order o)   { return o.log("captured"); }
    public Order   reserve(Order o)   { return o.withShipmentRef(reserveRef(o)); }
    public Order   label(Order o)     { return o.withTrackingLabel(print(o)); }
    public Order   notify(Order o)    { return o.withStatus("FULFILLED"); }

    // found by type: the arms take the Order parameters in fork order; the pre-fork order is Step.base()
    public Order merge(Order payment, Order shipping) {
        return Step.base(Order.class).withPaymentRef(payment.paymentRef())
                   .withShipmentRef(shipping.shipmentRef());
    }
}
// docs:end handlers
