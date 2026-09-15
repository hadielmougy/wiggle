package com.wiggle.docs;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.ForFlow;
import com.wiggle.client.worker.Worker;

/** The code on <a href="https://wiggle.sh/patterns/microservices/">wiggle.sh/patterns/microservices</a>. */
public final class MicroservicesSnippet {

    public record Order(String status, String receipt) {}

    // docs:begin contract
    interface OrderSteps {                         // one declaration, shared by every service
        Order validate(Order o);
        Order charge(Order o);
        Order renderReceipt(Order o);
        void  email(Order o);
    }
    // docs:end contract

    static FlowSpec define() {
        // docs:begin topology
        FlowSpec orders = FlowSpec.define("orders", Order.class, OrderSteps.class, (f, s) -> f
                .thenApply(s::validate, "orders")                  // queue: orders service
                .thenApply(s::charge, "payments")                  // queue: payments service
                .thenApply(s::renderReceipt, "gpu")                // queue: the GPU pool
                .thenAccept(s::email, "notify"));                  // queue: notifications
        // docs:end topology
        return orders;
    }

    /** Stands in for the payments service's own handler class. */
    @ForFlow("orders")
    static class PaymentHandlers {
        public Order charge(Order o) { return new Order("CHARGED", o.receipt()); }
    }

    static void startPaymentsService(WiggleClient client) {
        // docs:begin worker
        // payments-service (Java)
        new Worker(client, "payments-1")
                .registerHandler(new PaymentHandlers())   // only charge() matches a step it serves
                .start();
        // docs:end worker
    }

    private MicroservicesSnippet() {}
}
