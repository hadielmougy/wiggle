package dev.wiggle.binding;

import dev.wiggle.client.WiggleClient;
import dev.wiggle.client.worker.Worker;
import dev.wiggle.core.InstanceView;
import dev.wiggle.core.Json;
import dev.wiggle.server.ServerConfig;
import dev.wiggle.server.WiggleServer;

import java.time.Duration;
import java.util.Map;

import static dev.wiggle.binding.BindingOrder.put;

/**
 * Name-only binding, runnable. A workflow's topology is registered <b>once</b>, and independent
 * workers implement its steps by {@code (workflow, step)} name -- no worker re-declares the graph.
 * This process:
 * <ol>
 *   <li>starts an embedded server on :8080 and <b>registers the {@code binding-order} graph</b>
 *       (the author -- topology only);</li>
 *   <li>runs a <b>fulfilment</b> worker that binds every step but {@code charge} by name;</li>
 *   <li>runs a separate <b>payments</b> worker that binds only {@code charge} -- routed to the
 *       {@code payments} queue -- so ownership of that step is isolated;</li>
 *   <li>submits an order and prints the result.</li>
 * </ol>
 * Each worker reconciles its bindings against the registered graph on {@link Worker#start()},
 * discovering the queue each step polls. The same shape lets a worker in another language own a
 * step -- here both are Java.
 */
public final class BindingDemo {

    public static void main(String[] args) throws Exception {
        ServerConfig config = new ServerConfig(8080, "binding-node", null, null, null, 4,
                Duration.ofMillis(200), Duration.ofSeconds(2), 3, Duration.ofSeconds(30),
                Duration.ofSeconds(2), Duration.ofHours(1), 100, 0, Duration.ofSeconds(5), Duration.ofSeconds(10));

        try (WiggleServer server = new WiggleServer(config).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {

            // (1) author: register the topology, once.
            var blueprint = BindingOrder.blueprint();
            client.register(blueprint);
            System.out.println("[author] registered " + blueprint.name() + " v" + blueprint.version());

            // (2) fulfilment worker: implements everything except payments, by name.
            // (3) payments worker: owns only `charge`, on its own queue.
            try (Worker fulfilment = new Worker(client, "fulfilment-worker");
                 Worker payments = new Worker(client, "payments-worker")) {

                fulfilment.handle(BindingOrder.NAME, "validate", ctx -> put(ctx, "status", "VALIDATED"))
                          .handleGate(BindingOrder.NAME, "in-stock",
                                  ctx -> ((Number) Json.asObject(ctx).get("quantity")).intValue() > 0)
                          .handle(BindingOrder.NAME, "ship",
                                  ctx -> put(ctx, "trackingLabel", "DHL-" + Json.asObject(ctx).get("orderId")))
                          .handleEffect(BindingOrder.NAME, "notify",
                                  ctx -> System.out.println("   [fulfilment] notified " + Json.asObject(ctx).get("orderId")))
                          .start();   // reconciles: validates names/kinds, discovers queues
                System.out.println("[fulfilment] serving validate / in-stock / ship / notify by name");

                payments.handle(BindingOrder.NAME, "charge",
                                ctx -> put(ctx, "paymentRef", "auth-" + Json.asObject(ctx).get("orderId")))
                        .start();
                System.out.println("[payments]   serving charge on the payments queue");

                String id = client.start(BindingOrder.NAME, Map.of("orderId", "A-1001", "quantity", 2));
                InstanceView view = client.awaitCompletion(id, Duration.ofSeconds(30));
                Map<String, Object> ctx = Json.asObject(view.context());
                System.out.println("\n[result] status=" + view.status()
                        + " paymentRef=" + ctx.get("paymentRef")
                        + " tracking=" + ctx.get("trackingLabel"));
            }
        }
    }
}
