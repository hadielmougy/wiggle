package com.wiggle.docs;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.CompensableActivity;
import com.wiggle.client.worker.Compensation;
import com.wiggle.client.worker.ForFlow;
import com.wiggle.client.worker.Handles;
import com.wiggle.client.worker.Worker;

/** The code in {@code docs/saga-compensation.md}, the reference doc behind the saga pattern page. */
public final class SagaDocSnippet {

    public record Order(String authRef, String status) {}
    public record Payment(String reference) {}

    interface Gateway { Payment capture(String authRef); void refund(String ref); }
    interface Wms { String reserve(Order o); }

    // docs:begin contract
    interface OrderSteps {
        Order validate(Order o);
        CompensableActivity<Order, Payment> authorise();   // Order in, Payment out, and an undo
        Order printLabel(Order o);                  // does not
        // docs:elide
    }
    // docs:end contract

    interface FullOrderSteps extends OrderSteps {
        CompensableActivity<Order, Payment> capture();
        CompensableActivity<Order, Order> reserveStock();
        Order confirm(Order o);
    }

    static FlowSpec define() {
        // docs:begin topology
        FlowSpec orders = FlowSpec.define("order-fulfilment", 1, Order.class, FullOrderSteps.class, (f, s) -> f
                .thenApply(s::validate)
                .thenApplyCompensable(s::authorise)
                .thenApplyCompensable(s::capture)
                .thenApplyCompensable(s::reserveStock)
                .thenApply(s::printLabel)                  // no compensator — nothing to undo
                .thenApply(s::confirm));
        // docs:end topology
        return orders;
    }

    private SagaDocSnippet() {}
}
