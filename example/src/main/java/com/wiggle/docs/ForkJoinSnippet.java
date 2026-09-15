package com.wiggle.docs;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.flow.Wiggle;
import com.wiggle.client.worker.Context;
import com.wiggle.client.worker.ForFlow;
import com.wiggle.core.RetryPolicy;

import java.time.Duration;

/**
 * The code on <a href="https://wiggle.sh/patterns/fork-join/">wiggle.sh/patterns/fork-join</a>, and
 * the same topology quoted on the "why" page. See {@link SagaSnippet} for why these pages draw their
 * Java from compiled source.
 */
public final class ForkJoinSnippet {

    public record Order(String status, int quantity, String paymentRef, String shipmentRef,
                        String trackingLabel, String log) {
        Order withStatus(String s) { return new Order(s, quantity, paymentRef, shipmentRef, trackingLabel, log); }
        Order withPaymentRef(String p) { return new Order(status, quantity, p, shipmentRef, trackingLabel, log); }
        Order withShipmentRef(String s) { return new Order(status, quantity, paymentRef, s, trackingLabel, log); }
        Order withTrackingLabel(String t) { return new Order(status, quantity, paymentRef, shipmentRef, t, log); }
        Order log(String l) { return new Order(status, quantity, paymentRef, shipmentRef, trackingLabel, l); }
    }

    // docs:begin contract
    interface OrderSteps {                       // the steps, as a contract
        Order   validate(Order o);
        boolean inStock(Order o);
        Order   authorise(Order o);
        Order   capture(Order o);
        Order   reserveStock(Order o);
        Order   printLabel(Order o);
        Order   merge(@Context Order base, Order payment, Order shipping);
        Order   notify(Order o);
    }
    // docs:end contract

    static FlowSpec define() {
        // docs:begin topology
        FlowSpec orders = FlowSpec.define("order-fulfilment", Order.class, OrderSteps.class, (f, s) -> {
            var checked = f.thenApply(s::validate)
                    .thenFilter(s::inStock);         // false ⇒ the instance ends cleanly

            // continuing `checked` twice is the fan-out; the arms run on isolated copies
            var payment  = checked.thenApply(s::authorise, RetryPolicy.exponential(5, Duration.ofMillis(100)))
                                  .thenApply(s::capture);
            var shipping = checked.thenApply(s::reserveStock)
                                  .thenApply(s::printLabel);

            return Wiggle.allOf(payment, shipping)
                    .combineWithContext(s::merge)    // mandatory — there is no implicit join
                    .thenApply(s::notify);
        });
        // docs:end topology
        return orders;
    }

    private ForkJoinSnippet() {}
}
