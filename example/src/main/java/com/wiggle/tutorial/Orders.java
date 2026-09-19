package com.wiggle.tutorial;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.Context;
import com.wiggle.client.worker.Worker;
import com.wiggle.core.InstanceView;
import com.wiggle.core.RetryPolicy;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;

/**
 * The program the tutorial at <a href="https://wiggle.sh/tutorial/">wiggle.sh/tutorial</a> builds,
 * as source the compiler checks and {@code TutorialTest} runs end to end.
 *
 * <p>A tutorial is the one document a reader types out verbatim, so it is the worst place for code
 * that has never been compiled. Every block on those pages is a region of this file.
 */
public final class Orders {

    // docs:begin records
    public record Item(String sku, BigDecimal price) {}

    public record Order(String id, List<Item> items, BigDecimal total, String status) {
        Order withTotal(BigDecimal t) { return new Order(id, items, t, status); }
        Order withStatus(String s)    { return new Order(id, items, total, s); }
    }
    // docs:end records

    // docs:begin contract
    public interface OrderSteps {
        Order   validate(Order o);
        boolean inStock(Order o);                              // a gate: false ends the flow cleanly
        Item    price(Item item);                              // the element IS the branch's context
        Order   total(@Context Order base, List<Item> priced);  // the mandatory combine
        Order   confirm(Order o);
    }
    // docs:end contract

    // docs:begin topology
    public static FlowSpec spec() {
        return FlowSpec.define("orders", 1, Order.class, OrderSteps.class, (f, s) -> f
                .thenApply(s::validate)
                .thenFilter(s::inStock)
                .thenForEach(Order::items, item -> item
                        .thenApply(s::price, RetryPolicy.exponential(5, Duration.ofMillis(100))))
                .combine(s::total)
                .thenApply(s::confirm));
    }
    // docs:end topology

    // docs:begin main
    public static void main(String[] args) throws Exception {
        try (WiggleClient client = new WiggleClient("localhost:8080")) {
            FlowSpec orders = spec();
            client.register(orders);                        // publish the topology

            try (Worker worker = new Worker(client, "worker-1")
                    .registerHandler(new OrderHandlers())   // bind the code that runs the steps
                    .start()) {

                Order order = new Order("A-1001",
                        List.of(new Item("PEN", new BigDecimal("2.50")),
                                new Item("PAD", new BigDecimal("4.00"))),
                        BigDecimal.ZERO, "NEW");

                String id = client.start(orders, order);
                InstanceView done = client.awaitCompletion(id, Duration.ofSeconds(30));

                System.out.println(done.status() + " " + done.context());
            }
        }
    }
    // docs:end main

    private Orders() {}
}
