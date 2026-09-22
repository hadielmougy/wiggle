package com.wiggle.docs;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.observe.Observed;
import com.wiggle.observe.Observer;
import com.wiggle.observe.Run;

/** The code in {@code docs/observed-execution.md}. See {@link SagaSnippet} for why these live as source. */
public final class ObservedSnippet {

    public record Order(String id, boolean stocked) {}

    interface CheckoutSteps {
        Order validate(Order o);
        boolean inStock(Order o);
        Order charge(Order o);
    }

    /** The application's own implementation of its steps: plain code, no worker. */
    static final class Checkout implements CheckoutSteps {
        public Order validate(Order o) { return o; }
        public boolean inStock(Order o) { return o.stocked(); }
        public Order charge(Order o) { return o; }
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

    static void run(FlowSpec spec, Order order) {
        String orderId = order.id();
        // docs:begin usage
        try (Observer observer = Observer.connect("localhost:8080")) {
            Observed<CheckoutSteps> checkout = observer.observe(spec, CheckoutSteps.class, new Checkout());
            CheckoutSteps s = checkout.steps();          // the application's own object, wrapped

            try (Run run = checkout.begin(orderId)) {    // a run under a business key
                Order o = s.validate(order);
                if (s.inStock(o)) s.charge(o);
            }
        }
        // docs:end usage
    }

    private ObservedSnippet() {}
}
