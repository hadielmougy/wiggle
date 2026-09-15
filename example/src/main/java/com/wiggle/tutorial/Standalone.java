package com.wiggle.tutorial;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.Worker;
import com.wiggle.core.InstanceView;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;

/**
 * Tutorial 2: the server is its own process against your database; your app is a client and a
 * worker. See {@link Orders} for the flow, and {@link Embedded} for the single-process shape.
 */
public final class Standalone {

    // docs:begin submitter
    /** The submitter: registers the topology and starts instances. Owns no handlers. */
    static String submit(String serverUrl) throws Exception {
        try (WiggleClient client = new WiggleClient(serverUrl)) {
            FlowSpec orders = Orders.spec();
            client.register(orders);
            return client.start(orders, new Orders.Order("A-1001",
                    List.of(new Orders.Item("PEN", new BigDecimal("2.50")),
                            new Orders.Item("PAD", new BigDecimal("4.00"))),
                    BigDecimal.ZERO, "NEW"));
        }
    }
    // docs:end submitter

    // docs:begin worker
    /** The worker: a separate process that brings the code. It never defines the flow. */
    public static void main(String[] args) throws Exception {
        try (WiggleClient client = new WiggleClient("localhost:8080");
             Worker worker = new Worker(client, "worker-1")
                     .registerHandler(new OrderHandlers())
                     .start()) {

            System.out.println("worker-1 serving 'orders'; Ctrl-C to stop");
            Thread.currentThread().join();
        }
    }
    // docs:end worker

    static InstanceView await(String serverUrl, String id) throws Exception {
        try (WiggleClient client = new WiggleClient(serverUrl)) {
            return client.awaitCompletion(id, Duration.ofSeconds(30));
        }
    }

    private Standalone() {}
}
