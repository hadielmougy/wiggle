package com.wiggle.tutorial;

import com.wiggle.boot.WiggleNode;
import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.Worker;
import com.wiggle.core.InstanceView;
import com.wiggle.server.ServerConfig;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;

/**
 * Tutorial 1: the server, a worker and a client in one process, on your own database, to try the flow
 * with one main method. The server is the Boot application, run in this JVM through {@link WiggleNode};
 * in production it runs on its own, as {@link Standalone} shows. See {@link Orders} for the flow it runs
 * and why this lives as compiled source.
 */
public final class Embedded {

    // docs:begin main
    public static void main(String[] args) throws Exception {
        ServerConfig config = ServerConfig.fromEnvironment()
                .withStorage("jdbc:postgresql://localhost:5432/wiggle", "wiggle", "wiggle", 8)
                .withPort(8080);

        try (WiggleNode server = WiggleNode.start(config);
             WiggleClient client = new WiggleClient(server.baseUrl())) {

            FlowSpec orders = Orders.spec();
            client.register(orders);      // register BEFORE a worker binds: it binds at start()

            try (Worker worker = new Worker(client, "worker-1")
                    .registerHandler(new OrderHandlers())
                    .start()) {

                String id = client.start(orders, new Orders.Order("A-1001",
                        List.of(new Orders.Item("PEN", new BigDecimal("2.50")),
                                new Orders.Item("PAD", new BigDecimal("4.00"))),
                        BigDecimal.ZERO, "NEW"));

                InstanceView done = client.awaitCompletion(id, Duration.ofSeconds(30));
                System.out.println(done.status() + " " + done.context());
            }
        }
    }
    // docs:end main

    private Embedded() {}
}
