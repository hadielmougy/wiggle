package com.wiggle.tutorial;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.Worker;
import com.wiggle.core.InstanceView;
import com.wiggle.postgres.PostgresStorageFactory;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;

/**
 * Tutorial 1: the server embedded in your own JVM, on your own database. One process, one main
 * method. See {@link Orders} for the flow it runs and why this lives as compiled source.
 *
 * <p>An embedding app must use the <em>unshaded</em> modules -- {@code wiggle-client} plus a storage
 * module -- not {@code wiggle-client-all}. The shaded jar carries {@code com.wiggle.proto} compiled
 * against a relocated gRPC, and {@code wiggle-server} carries it compiled against the real one; both
 * on one classpath is a ClassCastException when the server registers its service.
 *
 * <p>Storage is an explicit factory rather than classpath discovery.
 * {@link PostgresStorageFactory} is the mapping the project ships and it arrives with
 * {@code wiggle-postgres}, so an embedding app needs no more than that -- and {@code StorageFactory}
 * is a functional interface, so one that wants a different mapping passes its own lambda.
 */
public final class Embedded {

    // docs:begin main
    public static void main(String[] args) throws Exception {
        ServerConfig config = ServerConfig.fromEnvironment()
                .withStorage("jdbc:postgresql://localhost:5432/wiggle", "wiggle", "wiggle", 8)
                .withPort(8080);

        try (WiggleServer server = new WiggleServer(config, new PostgresStorageFactory()).start();
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
