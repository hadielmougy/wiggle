package com.wiggle.docs;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.CompensableActivity;
import com.wiggle.observe.Observed;
import com.wiggle.observe.ObservedFlow;
import com.wiggle.observe.Observer;
import com.wiggle.observe.Run;

/** The code in {@code docs/observed-execution.md}. See {@link SagaSnippet} for why these live as source. */
public final class ObservedSnippet {

    public record Order(String id, boolean stocked) {}

    interface CheckoutSteps {
        Order validate(Order o);
        boolean inStock(Order o);
        CompensableActivity<Order, Order> charge();   // has an undo
    }

    /** The application's own implementation of its steps: plain code, no worker. */
    static final class Checkout implements CheckoutSteps {
        public Order validate(Order o) { return o; }
        public boolean inStock(Order o) { return o.stocked(); }
        public CompensableActivity<Order, Order> charge() {
            return new CompensableActivity<>() {
                public Order execute(Order o) { return o; }
                public void compensate(com.wiggle.client.worker.Compensation<Order, Order> c) { }
            };
        }
    }

    static FlowSpec define() {
        // docs:begin topology
        FlowSpec spec = FlowSpec.define("checkout", 1, Order.class, CheckoutSteps.class, (f, s) -> f
                .thenApply(s::validate)
                .thenFilter(s::inStock)
                .thenApplyCompensable(s::charge));   // no execution mode: an observer stamps OBSERVED when it publishes
        // docs:end topology
        return spec;
    }

    static void report(FlowSpec spec, String orderId, long startedAt, long finishedAt) {
        // docs:begin report
        try (Observer observer = Observer.connect("localhost:8080")) {
            ObservedFlow checkout = observer.publish(spec);        // stamps OBSERVED, registers, validates names

            checkout.record(orderId, "validate", startedAt, finishedAt);
            checkout.recordPredicate(orderId, "inStock", true, startedAt, finishedAt);
            checkout.recordError(orderId, "charge", "CardDeclined", startedAt, finishedAt);   // declares the run failed
            checkout.recordUndo(orderId, "charge", startedAt, finishedAt);                  // the compensation ran
        }
        // docs:end report
    }

    static void instrumented(FlowSpec spec, Order order) {
        String orderId = order.id();
        // docs:begin usage
        try (Observer observer = Observer.connect("localhost:8080")) {
            Observed<CheckoutSteps> checkout = observer.observe(spec, CheckoutSteps.class, new Checkout());
            CheckoutSteps s = checkout.steps();          // the application's own object, wrapped

            try (Run run = checkout.begin(orderId)) {    // a run under a business key
                Order o = s.validate(order);
                if (s.inStock(o)) s.charge().execute(o); // execute is the step; compensate would be its undo
            }
        }
        // docs:end usage
    }

    private ObservedSnippet() {}
}
