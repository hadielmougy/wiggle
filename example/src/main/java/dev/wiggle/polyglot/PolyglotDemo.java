package dev.wiggle.polyglot;

import dev.wiggle.client.worker.WiggleClient;
import dev.wiggle.client.worker.Worker;
import dev.wiggle.core.InstanceView;
import dev.wiggle.core.Json;
import dev.wiggle.server.ServerConfig;
import dev.wiggle.server.WiggleServer;

import java.time.Duration;
import java.util.Map;

import static dev.wiggle.polyglot.PolyglotOrder.put;

/**
 * The polyglot split, runnable. This process:
 * <ol>
 *   <li>starts an embedded server on :8080 and <b>registers the {@code polyglot-order} graph</b>
 *       (the author -- topology only);</li>
 *   <li>runs a Java worker that <b>binds most steps by name</b> via {@link Worker#handle} -- no
 *       blueprint, no re-declared topology -- serving the default queue;</li>
 *   <li>submits an order and waits.</li>
 * </ol>
 * The one step it does <i>not</i> implement is {@code charge}, routed to the {@code payments} queue.
 * Run the Python payments worker in another terminal to supply it:
 *
 * <pre>{@code   python clients/python/examples/polyglot_worker.py }</pre>
 *
 * When it binds {@code charge} by name and processes the step, this instance completes and the
 * result prints here. One flow, two languages, each owning its own steps.
 */
public final class PolyglotDemo {

    public static void main(String[] args) throws Exception {
        ServerConfig config = new ServerConfig(8080, "polyglot-node", null, null, null, 4,
                Duration.ofMillis(200), Duration.ofSeconds(2), 3, Duration.ofSeconds(30),
                Duration.ofSeconds(2), Duration.ofHours(1), 100, 0, Duration.ofSeconds(5), Duration.ofSeconds(10));

        try (WiggleServer server = new WiggleServer(config).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {

            // (1) author: register the topology, once.
            var blueprint = PolyglotOrder.blueprint();
            client.register(blueprint);
            System.out.println("[java-author] registered " + blueprint.name() + " v" + blueprint.version());

            // (2) implement, by name -- the Java team owns everything except payments.
            try (Worker worker = new Worker(client, "java-worker")) {
                worker.handle(PolyglotOrder.NAME, "validate", ctx -> put(ctx, "status", "VALIDATED"))
                      .handleGate(PolyglotOrder.NAME, "in-stock",
                              ctx -> ((Number) Json.asObject(ctx).get("quantity")).intValue() > 0)
                      .handle(PolyglotOrder.NAME, "ship",
                              ctx -> put(ctx, "trackingLabel", "DHL-" + Json.asObject(ctx).get("orderId")))
                      .handleEffect(PolyglotOrder.NAME, "notify",
                              ctx -> System.out.println("   [java-worker] notified " + Json.asObject(ctx).get("orderId")));
                worker.start();   // reconciles against the registered graph; discovers its queue(s)
                System.out.println("[java-worker] serving validate / in-stock / ship / notify by name");

                // (3) submit and wait for the Python worker to supply `charge`.
                String id = client.start(PolyglotOrder.NAME, Map.of("orderId", "A-1001", "quantity", 2));
                System.out.println("\n>>> waiting for the Python payments worker to implement `charge` ...");
                System.out.println(">>> run:  python clients/python/examples/polyglot_worker.py\n");

                InstanceView view = client.awaitCompletion(id, Duration.ofMinutes(2));
                Map<String, Object> ctx = Json.asObject(view.context());
                System.out.println("[result] status=" + view.status()
                        + " paymentRef=" + ctx.get("paymentRef")
                        + " tracking=" + ctx.get("trackingLabel"));
            }
        }
    }
}
