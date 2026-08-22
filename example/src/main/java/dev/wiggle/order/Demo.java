package dev.wiggle.order;

import dev.wiggle.client.dsl.Blueprint;
import dev.wiggle.client.worker.WiggleClient;
import dev.wiggle.client.worker.Worker;
import dev.wiggle.core.InstanceView;
import dev.wiggle.server.WiggleServer;
import dev.wiggle.server.ServerConfig;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;

/**
 * End-to-end demo in a single JVM: one server node, one worker, three instances that
 * exercise the happy path, the filter path and the failure path.
 *
 * Run the pieces separately with {@code :server:run} and {@code :example:runWorker}.
 */
public final class Demo {

    public static void main(String[] args) throws Exception {
        ServerConfig config = new ServerConfig(0, "demo-node", null, null, null, 4,
                Duration.ofMillis(200), Duration.ofSeconds(2), 3, Duration.ofSeconds(30),
                Duration.ofSeconds(2), Duration.ofHours(1), 100, 0, Duration.ofSeconds(5), Duration.ofSeconds(10));

        try (WiggleServer server = new WiggleServer(config).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {

            Blueprint<Order> blueprint = OrderFulfilment.blueprint();
            System.out.println("Workflow " + blueprint.name() + " v" + blueprint.version()
                    + " compiled to " + blueprint.definition().nodes().size() + " nodes");

            try (Worker worker = new Worker(client, "worker-1").register(blueprint)) {
                worker.start();

                System.out.println("\n--- happy path (retries through a flaky gateway) ---");
                String happy = client.start(blueprint, Order.of("A-1001", "hadi", 3, new BigDecimal("249.90")));
                print(client.awaitCompletion(happy, Duration.ofSeconds(30)));

                System.out.println("\n--- filtered (quantity 0 short-circuits the pipeline) ---");
                String filtered = client.start(blueprint, Order.of("A-1002", "hadi", 0, new BigDecimal("10.00")));
                print(client.awaitCompletion(filtered, Duration.ofSeconds(30)));

                System.out.println("\n--- failure (validation rejects the order for good) ---");
                String failed = client.start(blueprint, Order.of("A-1003", "  ", 1, new BigDecimal("5.00")));
                print(client.awaitCompletion(failed, Duration.ofSeconds(30)));

                System.out.println("\n--- cluster ---");
                Map<String, Object> cluster = client.cluster();
                System.out.println("   leader on this node: " + cluster.get("leader"));
                System.out.println("   members: " + cluster.get("members"));
            }
        }
    }

    private static void print(InstanceView v) {
        System.out.println("   instance " + v.id());
        System.out.println("   status:  " + v.status()
                + (v.terminationReason() == null ? "" : " (" + v.terminationReason() + ")"));
        if (v.error() != null) System.out.println("   error:   " + v.error());
        System.out.println("   context: " + v.context());
    }
}
