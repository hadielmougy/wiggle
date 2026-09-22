package com.wiggle.docs;

import com.wiggle.client.flow.FlowSpec;

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

    private ObservedSnippet() {}
}
