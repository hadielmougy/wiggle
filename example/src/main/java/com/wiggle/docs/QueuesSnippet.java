package com.wiggle.docs;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.ForFlow;
import com.wiggle.client.worker.Worker;
import com.wiggle.client.worker.WorkerOptions;
import com.wiggle.core.criteria.AttrType;
import com.wiggle.core.criteria.QueueCriteria;
import com.wiggle.core.criteria.Task_;

/** The code in {@code docs/queues.md}. See {@link SagaSnippet} for why these live as source. */
public final class QueuesSnippet {

    public record Order(String status) {}

    public record Customer(String region) {}

    public record RegionalOrder(Customer customer, java.math.BigDecimal total) {}

    interface RegionalSteps {
        RegionalOrder validate(RegionalOrder o);
        RegionalOrder charge(RegionalOrder o);
    }

    @ForFlow("regional-orders")
    static class RegionalHandlers {
        public RegionalOrder validate(RegionalOrder o) { return o; }
        public RegionalOrder charge(RegionalOrder o) { return o; }
    }

    interface OrderSteps {
        Order validate(Order o);
        Order charge(Order o);
        Order renderReceipt(Order o);
        Order email(Order o);
    }

    static FlowSpec define() {
        // docs:begin topology
        FlowSpec orders = FlowSpec.define("orders", 1, Order.class, OrderSteps.class, (f, s) -> f
                .thenApply(s::validate)                    // queue "orders" (the default)
                .thenApply(s::charge, "payments")          // queue "payments"
                .thenApply(s::renderReceipt, "gpu")        // queue "gpu"
                .thenApply(s::email, "notify"));           // queue "notify"
        // docs:end topology
        return orders;
    }

    @ForFlow("orders")
    static class GpuHandlers {
        public Order renderReceipt(Order o) { return o; }
    }

    @ForFlow("orders")
    static class OrderHandlers {
        public Order validate(Order o) { return o; }
        public Order charge(Order o) { return o; }
        public Order renderReceipt(Order o) { return o; }
        public Order email(Order o) { return o; }
    }

    static void specialised(WiggleClient client) {
        // docs:begin specialised-worker
        // gpu-render-pool: a service that ONLY runs the "gpu" steps
        Worker gpu = new Worker(client, "gpu-1",
                        WorkerOptions.defaults().withQueues("gpu"))   // specialization
                .registerHandler(new GpuHandlers())   // fetches the graph by name; only claims gpu-queue steps
                .start();
        // docs:end specialised-worker
        gpu.close();
    }

    static void criteria(WiggleClient client) {
        // docs:begin criteria-worker
        // payments-eu: on "payments", only charges of orders keyed "eu-..."; on "refunds", everything
        QueueCriteria euCharges = QueueCriteria.from("payments")
                .where(cb -> cb.and(
                        cb.equal(Task_.STEP, "charge"),
                        cb.like(Task_.CORRELATION_ID, "eu-%"),
                        cb.lessThan(Task_.ATTEMPT, 4)));
        QueueCriteria refunds = QueueCriteria.from("refunds");
        Worker eu = new Worker(client, "payments-eu-1",
                        WorkerOptions.defaults().withCriteria(euCharges, refunds))
                .registerHandler(new OrderHandlers())
                .start();
        // docs:end criteria-worker
        eu.close();
    }

    static void attributes(WiggleClient client) {
        // docs:begin attributes
        FlowSpec orders = FlowSpec.define("regional-orders", 1, RegionalOrder.class, RegionalSteps.class, (f, s) -> f
                .attribute("region", AttrType.STRING, "customer.region")   // read from each step's context
                .attribute("total", AttrType.DECIMAL, "total")
                .thenApply(s::validate)
                .thenApply(s::charge, "payments"));

        // payments-eu-large: EU charges of 1000 or more
        QueueCriteria euLarge = QueueCriteria.from("payments")
                .where(cb -> cb.and(
                        cb.equal(Task_.attr("region", String.class), "eu"),
                        cb.greaterThanOrEqualTo(Task_.attr("total", java.math.BigDecimal.class),
                                new java.math.BigDecimal("1000"))));
        Worker large = new Worker(client, "payments-eu-large-1",
                        WorkerOptions.defaults().withCriteria(euLarge))
                .registerHandler(new RegionalHandlers())
                .start();
        // docs:end attributes
        client.register(orders);
        large.close();
    }

    static void general(WiggleClient client) {
        // docs:begin general-worker
        // order-service: default = serve every queue its bound steps live on
        Worker general = new Worker(client, "order-1").registerHandler(new OrderHandlers()).start();
        // docs:end general-worker
        general.close();
    }

    private QueuesSnippet() {}
}
