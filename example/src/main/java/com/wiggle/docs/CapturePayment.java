package com.wiggle.docs;

import com.wiggle.client.worker.CompensableActivity;
import com.wiggle.client.worker.Compensation;
import com.wiggle.docs.SagaDocSnippet.Gateway;
import com.wiggle.docs.SagaDocSnippet.Order;
import com.wiggle.docs.SagaDocSnippet.Payment;

/** The activity+undo class quoted in {@code docs/saga-compensation.md}. */
// docs:begin activity
final class CapturePayment implements CompensableActivity<Order, Payment> {
    // docs:skip
    private final Gateway gateway;

    CapturePayment(Gateway gateway) { this.gateway = gateway; }
    // docs:resume
    public Payment execute(Order o) { return gateway.capture(o.authRef()); }
    public void    compensate(Compensation<Order, Payment> c) {
        gateway.refund(c.result().reference());    // result(): the step's own product — idempotent!
        // c.input() is also available: restore-style undos and undo-only data (idempotency keys)
        // read from the INPUT snapshot, so nothing is smuggled through the business context.
    }
}
// docs:end activity
