package com.wiggle.docs;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.observe.ObservedFlow;
import com.wiggle.observe.Observer;

/** The code in {@code docs/observed-execution.md}. See {@link SagaSnippet} for why these live as source. */
public final class ObservedSnippet {

    public record Order(String id, boolean stocked) {}

    interface CheckoutSteps {
        Order validate(Order o);
        boolean inStock(Order o);
        Order charge(Order o);
    }

    static FlowSpec define() {
        // docs:begin topology
        FlowSpec spec = FlowSpec.define("checkout", 1, Order.class, CheckoutSteps.class, (f, s) -> f
                .thenApply(s::validate)
                .thenFilter(s::inStock)
                .thenApply(s::charge));   // no execution mode: an observer stamps OBSERVED when it publishes
        // docs:end topology
        return spec;
    }

    static void report(FlowSpec spec, String orderId, long startedAt, long finishedAt) {
        // docs:begin report
        try (Observer observer = Observer.connect("localhost:8080")) {
            ObservedFlow checkout = observer.publish(spec);        // stamps OBSERVED, registers, validates names

            checkout.record(orderId, "validate", startedAt, finishedAt);
            checkout.recordPredicate(orderId, "inStock", true, startedAt, finishedAt);
            checkout.recordError(orderId, "charge", "CardDeclined", startedAt, finishedAt);
        }
        // docs:end report
    }

    private ObservedSnippet() {}
}
