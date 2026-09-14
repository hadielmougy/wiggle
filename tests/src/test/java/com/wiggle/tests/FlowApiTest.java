package com.wiggle.tests;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;

import com.wiggle.client.flow.Wiggle;
import com.wiggle.client.worker.Handlers;
import com.wiggle.client.worker.Worker;
import com.wiggle.client.worker.WorkerOptions;
import com.wiggle.core.InstanceView;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * End-to-end proof for the future-shaped API in {@link com.wiggle.client.flow}: a workflow defined as
 * a chain of method references really does run on a worker. The claim under test is the one the unit
 * tests cannot make -- that the node names derived from {@code handlers::method} are exactly the names
 * the worker binds back to those same methods, through a real server, fork isolation and combine
 * included.
 *
 * <p>Note that one object is used twice: it defines the topology (as the receiver of the references)
 * and it implements the steps (as the {@link Handlers @Handlers} object). That is the point of the
 * API -- the two halves cannot drift, because the compiler ties them together.
 */
class FlowApiTest {

    public record Order(String id, int quantity, String status) {}

    public record Payment(String reference) {}

    public record Label(String code) {}

    public record Fulfilment(String orderId, String paymentRef, String labelCode) {}

    /** The steps of flow-order, as a contract: what the spec names, and what the handler implements. */
    public interface OrderFlowSteps {
        Order validate(Order o);
        boolean inStock(Order o);
        Payment charge(Order o);
        Label label(Order o);
        Fulfilment settle(Payment payment, Label label);
        void notifyCustomer(Fulfilment f);
    }

    @Handlers("flow-order")
    public static final class OrderFlow implements OrderFlowSteps {

        final AtomicReference<Fulfilment> notified = new AtomicReference<>();

        @Override
        public Order validate(Order o) { return new Order(o.id(), o.quantity(), "VALIDATED"); }

        @Override
        public boolean inStock(Order o) { return o.quantity() > 0; }

        @Override
        public Payment charge(Order o) { return new Payment("auth-" + o.id()); }

        @Override
        public Label label(Order o) { return new Label("lbl-" + o.id()); }

        @Override
        public Fulfilment settle(Payment payment,
                                 Label label) {
            return new Fulfilment(payment.reference().substring("auth-".length()),
                    payment.reference(), label.code());
        }

        @Override
        public void notifyCustomer(Fulfilment f) { notified.set(f); }
    }

    /** The workflow, written as a chain of references to {@code flow}'s own methods. */
    private static FlowSpec flowSpec() {
        return Wiggle.define("flow-order", Order.class, OrderFlowSteps.class, (f, s) -> {
            var validated = f.thenApply(s::validate).thenFilter(s::inStock);

            var payment = validated.thenApply(s::charge);
            var shipping = validated.thenApply(s::label);

            return Wiggle.allOf(payment, shipping)
                    .combine(s::settle)
                    .thenAccept(s::notifyCustomer);
        });
    }

    @Test
    @DisplayName("a flow defined by method references registers, binds and runs to completion")
    void methodReferenceNamesBindToTheirOwnMethodsOnAWorker() throws Exception {
        OrderFlow flow = new OrderFlow();

        Map<String, Object> out = run(flowSpec(), flow, Map.of("id", "o1", "quantity", 2));

        // the combine's return is the whole post-join context, so these are the fields that survived
        assertEquals("o1", out.get("orderId"));
        assertEquals("auth-o1", out.get("paymentRef"), "the payment arm ran and its result reached settle");
        assertEquals("lbl-o1", out.get("labelCode"), "the shipping arm ran and its result reached settle");
        assertNull(out.get("status"), "a combine's return replaces the context; it does not merge into it");

        assertEquals(new Fulfilment("o1", "auth-o1", "lbl-o1"), flow.notified.get(),
                "the effect step bound to notifyCustomer and saw the final context");
    }

    public interface PositionalSteps {
        Order validate(Order o);
        Payment charge(Order o);
        Label label(Order o);
        Fulfilment settle(Payment payment, Label label);
    }

    @Handlers("positional-order")
    public static final class PositionalOrderFlow implements PositionalSteps {

        public Order validate(Order o) { return new Order(o.id(), o.quantity(), "VALIDATED"); }

        public Payment charge(Order o) { return new Payment("auth-" + o.id()); }

        public Label label(Order o) { return new Label("lbl-" + o.id()); }

        /** The arms bind by position, in the order they were given to allOf. */
        public Fulfilment settle(Payment payment, Label label) {
            return new Fulfilment(payment.reference().substring("auth-".length()),
                    payment.reference(), label.code());
        }
    }

    @Test
    @DisplayName("a combine binds its arms by fork order, end to end")
    void positionalCombineBindsArmsInForkOrder() throws Exception {
        PositionalOrderFlow flow = new PositionalOrderFlow();

        FlowSpec bp = Wiggle.define("positional-order", Order.class, PositionalSteps.class, (f, s) -> {
            var validated = f.thenApply(s::validate);
            var payment = validated.thenApply(s::charge);
            var shipping = validated.thenApply(s::label);
            return Wiggle.allOf(payment, shipping).combine(s::settle);
        });

        Map<String, Object> out = run(bp, flow, Map.of("id", "o3", "quantity", 1));

        assertEquals("auth-o3", out.get("paymentRef"), "the first arm reached the first parameter");
        assertEquals("lbl-o3", out.get("labelCode"), "the second arm reached the second parameter");
    }

    @Test
    @DisplayName("a gate written as thenFilter still short-circuits the instance")
    void gateShortCircuitsWhenTheReferencedGuardIsFalse() throws Exception {
        OrderFlow flow = new OrderFlow();

        Map<String, Object> out = run(flowSpec(), flow, Map.of("id", "o2", "quantity", 0));

        assertEquals("VALIDATED", out.get("status"), "validate ran before the gate closed");
        assertNull(out.get("paymentRef"), "the gate closed, so the fork never ran");
        assertNull(flow.notified.get(), "and neither did the effect after the combine");
    }

    /** Runs a single instance to completion on a one-node server ({@link TestStorage}) and returns
     *  its context. */
    private static Map<String, Object> run(FlowSpec bp, Object handlers, Map<String, Object> input) throws Exception {
        com.wiggle.server.ServerConfig config = new com.wiggle.server.ServerConfig(
                0, "node-0", TestStorage.url("flowapi"), TestStorage.user(), TestStorage.password(), 8,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0, Duration.ofSeconds(5), Duration.ofSeconds(10));
        try (com.wiggle.server.WiggleServer server =
                     new com.wiggle.server.WiggleServer(config, new com.wiggle.dist.WiggleStorageFactory()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            Worker w = new Worker(client, "w-" + System.nanoTime(),
                    WorkerOptions.defaults().withConcurrency(4).withLongPollWait(Duration.ofMillis(250)));
            client.register(bp);
            w.handlers(handlers);
            w.start();
            try {
                String id = client.start(bp, input);
                InstanceView v = client.awaitCompletion(id, Duration.ofSeconds(30));
                return asMap(v.context());
            } finally {
                w.close();
            }
        }
    }

    private static Map<String, Object> asMap(Object ctx) {
        return com.wiggle.core.Json.asObject(ctx);
    }
}
