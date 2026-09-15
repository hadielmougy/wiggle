package com.wiggle.docs;

import com.wiggle.client.worker.CompensableActivity;
import com.wiggle.client.worker.Compensation;
import com.wiggle.client.worker.ForFlow;
import com.wiggle.client.worker.Handles;
import com.wiggle.docs.SagaDocSnippet.Gateway;
import com.wiggle.docs.SagaDocSnippet.Order;
import com.wiggle.docs.SagaDocSnippet.Payment;
import com.wiggle.docs.SagaDocSnippet.Wms;

/** The handler class quoted in {@code docs/saga-compensation.md}. */
// docs:begin handlers
@ForFlow("order-fulfilment")
class SagaDocHandlers {
    // docs:skip
    private Gateway gateway;
    private Wms wms;
    // docs:resume
    public boolean inStock(Order o) { return true; }        // plain methods coexist

    public CompensableActivity<Order, Payment> capturePayment() {   // factory -> serves "capturePayment"
        return new CapturePayment(gateway);
    }

    @Handles("reserveStock")                                // rename when the method name can't match
    public CompensableActivity<Order, Order> stockReserver() { return new ReserveStock(wms); }
}
// docs:end handlers
