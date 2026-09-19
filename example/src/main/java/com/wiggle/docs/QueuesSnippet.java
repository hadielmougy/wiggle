package com.wiggle.docs;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.ForFlow;
import com.wiggle.client.worker.Worker;
import com.wiggle.client.worker.WorkerOptions;

/** The code in {@code docs/queues.md}. See {@link SagaSnippet} for why these live as source. */
public final class QueuesSnippet {

    public record Order(String status) {}

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

    static void general(WiggleClient client) {
        // docs:begin general-worker
        // order-service: default = serve every queue its bound steps live on
        Worker general = new Worker(client, "order-1").registerHandler(new OrderHandlers()).start();
        // docs:end general-worker
        general.close();
    }

    private QueuesSnippet() {}
}
