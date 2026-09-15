package com.wiggle.docs;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.ForFlow;
import com.wiggle.client.worker.PermanentActivityException;
import com.wiggle.core.RetryPolicy;

import java.time.Duration;

/**
 * The code in {@code docs/error-handling.md}. See {@link SagaSnippet} for why these live as source;
 * {@code ErrorHandlingTest} runs every claim the page makes.
 */
public final class ErrorHandlingSnippet {

    public record Order(String id, int quantity, String status) {
        Order withStatus(String s) { return new Order(id, quantity, s); }
    }

    interface Gateway { void charge(Order o); }

    static class CardDeclinedException extends RuntimeException {
        CardDeclinedException(String m) { super(m); }
    }

    /** The gateway call, wrapping a cause the step should not retry. */
    static void chargeOrFailPermanently(Gateway gateway, Order o) {
        // docs:begin wrap
        try {
            gateway.charge(o);
        } catch (CardDeclinedException e) {
            throw new PermanentActivityException("declined for " + o.id(), e);
        }
        // docs:end wrap
    }

    public interface OrderSteps {
        Order   validate(Order o);
        boolean inStock(Order o);
        Order   charge(Order o);
        Order   confirm(Order o);
    }

    public static FlowSpec spec() {
        // docs:begin policies
        return FlowSpec.define("orders", Order.class, OrderSteps.class, (f, s) -> f
                // no policy: inherits the workflow default, which is retry forever
                .thenApply(s::validate)
                // a gate is not an error -- false ends the instance successfully
                .thenFilter(s::inStock)
                // this one talks to a payment gateway, so back off rather than hammer it
                .thenApply(s::charge, RetryPolicy.exponential(5, Duration.ofMillis(100)))
                // and this one is not worth retrying at all
                .thenApply(s::confirm, RetryPolicy.none()));
        // docs:end policies
    }

    // docs:begin permanent
    @ForFlow("orders")
    public static class OrderHandlers implements OrderSteps {
        // docs:skip
        private Gateway gateway;
        // docs:resume

        @Override public Order validate(Order o) {
            if (o.quantity() <= 0) {
                // retrying cannot make this order valid: fail now, no attempts left on the clock
                throw new PermanentActivityException("order " + o.id() + " has no items");
            }
            return o.withStatus("VALIDATED");
        }

        @Override public boolean inStock(Order o) {
            return o.quantity() <= 50;      // false: nothing to do, end cleanly
        }

        @Override public Order charge(Order o) {
            gateway.charge(o);              // throws on a network blip -> retried per the policy
            return o.withStatus("CHARGED");
        }

        @Override public Order confirm(Order o) { return o.withStatus("CONFIRMED"); }
    }
    // docs:end permanent

    static void stop(WiggleClient client, String instanceId) {
        // docs:begin cancel
        client.cancel(instanceId, "customer withdrew the order");
        // docs:end cancel
    }

    private ErrorHandlingSnippet() {}
}
