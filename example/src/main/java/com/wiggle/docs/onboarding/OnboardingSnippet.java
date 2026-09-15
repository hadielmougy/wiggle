package com.wiggle.docs.onboarding;

import com.wiggle.client.WiggleClient;
import com.wiggle.docs.SagaSnippet;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.flow.Wiggle;
import com.wiggle.client.worker.Context;
import com.wiggle.client.worker.Worker;
import com.wiggle.client.worker.WorkerOptions;
import com.wiggle.core.InstanceView;
import com.wiggle.core.RetryPolicy;

import java.time.Duration;
import java.util.Map;

import static java.time.Duration.ofMillis;

/** The code in {@code docs/onboarding.md}. See {@link SagaSnippet} for why these live as source. */
public final class OnboardingSnippet {

    public record Order(String orderId, int quantity, String status, String paymentRef,
                        String shipmentRef, String trackingLabel, String note) {
        static Order of(String id) { return new Order(id, 1, null, null, null, null, null); }
        Order withStatus(String s) { return new Order(orderId, quantity, s, paymentRef, shipmentRef, trackingLabel, note); }
        Order withPaymentRef(String p) { return new Order(orderId, quantity, status, p, shipmentRef, trackingLabel, note); }
        Order withShipmentRef(String s) { return new Order(orderId, quantity, status, paymentRef, s, trackingLabel, note); }
        Order withTrackingLabel(String t) { return new Order(orderId, quantity, status, paymentRef, shipmentRef, t, note); }
        Order log(String l) { return new Order(orderId, quantity, status, paymentRef, shipmentRef, trackingLabel, l); }
    }

    // docs:begin contract
    interface OrderSteps {
        Order   validate(Order o);
        boolean inStock(Order o);
        Order   authorise(Order o);
        Order   merge(@Context Order base, Order payment, Order shipping);
        // docs:elide     ...
        // docs:skip
        Order   capture(Order o);
        Order   reserve(Order o);
        Order   label(Order o);
        Order   notify(Order o);
        // docs:resume
    }
    // docs:end contract

    static FlowSpec registerLine() {
        // docs:begin register-line
        // the author registers this without implementing a single step
        FlowSpec orders = FlowSpec.define("order-fulfilment", Order.class, OrderSteps.class,
                // docs:elide         (f, s) -> { … });
                // docs:skip
                (f, s) -> f.thenApply(s::validate));
                // docs:resume
        // docs:end register-line
        return orders;
    }

    static FlowSpec define() {
        // docs:begin topology
        FlowSpec orders = FlowSpec.define("order-fulfilment", Order.class, OrderSteps.class, (f, s) -> {
            var validated = f.thenApply(s::validate).thenFilter(s::inStock);

            var payment  = validated.thenApply(s::authorise, RetryPolicy.exponential(5, ofMillis(100)))
                                    .thenApply(s::capture);
            var shipping = validated.thenApply(s::reserve)
                                    .thenSleep("await", ofMillis(300))
                                    .thenApply(s::label);

            return Wiggle.allOf(payment, shipping)   // continuing `validated` twice is the fan-out
                    .combineWithContext(s::merge)    // arms are isolated, so rejoining is always explicit
                    .thenApply(s::notify);
        });
        // docs:end topology
        return orders;
    }

    static void lifecycle(FlowSpec orders) throws Exception {
        // docs:begin client-lifecycle
        try (WiggleClient client = new WiggleClient("localhost:8080")) {
            String id = client.start(orders, Order.of("A-1001"));                  // same-JVM convenience: by FlowSpec
            InstanceView v = client.awaitCompletion(id, Duration.ofSeconds(30));   // COMPLETED | FAILED | CANCELLED
            client.cancel(id, "reason");
        }
        // docs:end client-lifecycle
    }

    static void byName(WiggleClient client, Map<String, Object> ctx) {
        // docs:begin start-by-name
        String id = client.start("order-fulfilment", Map.of("orderId", "A-1001", "quantity", 3L));

        // version pinning: unpinned = latest registered; pin to be immune to mid-deploy changes.
        String id2 = client.start("order-fulfilment", ctx, 302800684, "corr-42");
        // docs:end start-by-name
    }

    static void versionScoped(WiggleClient client, FlowSpec v1, FlowSpec v2) {
        // docs:begin version-pinning
        new Worker(client, "service-a").registerHandler(new OrderHandlers(), v1.version());  // claims only v1
        new Worker(client, "service-b").registerHandler(new OrderHandlers(), v2.version());  // claims only v2
        // docs:end version-pinning
    }

    static void tuned(WiggleClient client) {
        // docs:begin worker-options
        new Worker(client, "worker-1", WorkerOptions.defaults()
                .withConcurrency(16)
                .withLease(Duration.ofSeconds(30))
                .withLongPollWait(Duration.ofSeconds(10))
                .withLocalBatchSize(64));
        // docs:end worker-options
    }

    private OnboardingSnippet() {}
}
