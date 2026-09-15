package com.wiggle.tutorial;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.Worker;
import com.wiggle.core.InstanceView;
import com.wiggle.jdbc.JdbcStorage;
import com.wiggle.postgres.PostgresDialect;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import com.wiggle.server.store.StorageFactory;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;

/**
 * Tutorial 1: the server embedded in your own JVM, on your own database. One process, one main
 * method. See {@link Orders} for the flow it runs and why this lives as compiled source.
 */
public final class Embedded {

    // docs:begin storage
    /** Storage is an explicit factory, not classpath discovery -- so an embedding app writes it. */
    static StorageFactory postgres() {
        return config -> new JdbcStorage(config.jdbcUrl(), config.jdbcUser(), config.jdbcPassword(),
                config.jdbcPoolSize(), new PostgresDialect());
    }
    // docs:end storage

    // docs:begin main
    public static void main(String[] args) throws Exception {
        ServerConfig config = ServerConfig.fromEnvironment()
                .withStorage("jdbc:postgresql://localhost:5432/wiggle", "wiggle", "wiggle", 8)
                .withPort(8080);

        try (WiggleServer server = new WiggleServer(config, postgres()).start();
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
