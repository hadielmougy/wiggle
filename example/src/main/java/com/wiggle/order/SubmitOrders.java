package com.wiggle.order;

import com.wiggle.client.CellResolver;
import com.wiggle.core.InstanceView;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Submits a batch of orders and waits for them. Usage: SubmitOrders [count].
 *
 * <p>Connecting starts from {@link CellResolver#direct(String)} -- the same entry point the sharded
 * {@link NamespaceSubmitter} uses via {@code coordinator(...)}; only the factory differs.
 */
public final class SubmitOrders {

    public static void main(String[] args) {
        String url = System.getenv().getOrDefault("WIGGLE_URL", "localhost:8080");
        int count = args.length > 0 ? Integer.parseInt(args[0]) : 1;

        try (CellResolver wiggle = CellResolver.direct(url)) {
            var client = wiggle.client();
            // Registering here too means orders can be submitted before any worker exists;
            // they simply queue until one shows up.
            client.register(OrderFulfilment.blueprint());

            List<String> ids = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                Order order = Order.of("A-" + (2000 + i), "customer-" + i, 1 + (i % 3),
                        new BigDecimal("100.00").add(BigDecimal.valueOf(i)));
                ids.add(client.start(OrderFulfilment.blueprint(), order));
            }
            System.out.println("submitted " + count + " orders");

            for (String id : ids) {
                InstanceView v = client.awaitCompletion(id, Duration.ofMinutes(2));
                System.out.println("  " + id + "  " + v.status()
                        + (v.error() == null ? "" : "  " + v.error()));
            }
        }
    }
}
