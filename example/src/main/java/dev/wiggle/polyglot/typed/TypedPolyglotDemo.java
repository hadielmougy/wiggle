package dev.wiggle.polyglot.typed;

import dev.wiggle.client.WiggleClient;
import dev.wiggle.client.worker.Worker;
import dev.wiggle.core.ContextCodec;
import dev.wiggle.core.InstanceView;
import dev.wiggle.server.ServerConfig;
import dev.wiggle.server.WiggleServer;

import java.time.Duration;

/**
 * The polyglot split with a <b>typed</b> context. Java authors and serves the flow using a
 * {@link Purchase} record (via {@link ContextCodec#records}); Python serves the one payments step as
 * a plain dict. Both work on the same instance because the record and the dict are the same JSON on
 * the wire — interop is by activity name, not by type.
 *
 * <ol>
 *   <li>starts an embedded server on :8080 and registers the {@code typed-order} graph;</li>
 *   <li>runs a Java worker that binds every step but {@code charge} by name, with <b>typed</b>
 *       handlers ({@code Purchase -> Purchase});</li>
 *   <li>submits a {@link Purchase} and waits.</li>
 * </ol>
 * Supply {@code charge} from the other language:
 * <pre>{@code   python clients/python/examples/polyglot_typed_worker.py }</pre>
 */
public final class TypedPolyglotDemo {

    public static void main(String[] args) throws Exception {
        ServerConfig config = new ServerConfig(8080, "typed-polyglot-node", null, null, null, 4,
                Duration.ofMillis(200), Duration.ofSeconds(2), 3, Duration.ofSeconds(30),
                Duration.ofSeconds(2), Duration.ofHours(1), 100, 0, Duration.ofSeconds(5), Duration.ofSeconds(10));

        ContextCodec<Purchase> codec = TypedPolyglotOrder.codec();

        try (WiggleServer server = new WiggleServer(config).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {

            // (1) author: register the topology, once.
            var blueprint = TypedPolyglotOrder.blueprint();
            client.register(blueprint);
            System.out.println("[java-author] registered " + blueprint.name() + " v" + blueprint.version());

            // (2) implement, by name, with TYPED handlers — the Java team owns everything but payments.
            try (Worker worker = new Worker(client, "java-typed-worker")) {
                worker.handle(TypedPolyglotOrder.NAME, "validate", codec, p -> p.withStatus("VALIDATED"))
                      .handleGate(TypedPolyglotOrder.NAME, "in-stock", codec, p -> p.quantity() > 0)
                      .handleEffect(TypedPolyglotOrder.NAME, "notify", codec,
                              p -> System.out.println("   [java-worker] notified " + p.orderId()
                                      + " status=" + p.status() + " payment=" + p.paymentRef()));
                worker.start();
                System.out.println("[java-worker] serving validate / in-stock / notify as typed Purchase handlers");

                // (3) submit a typed Purchase and wait for the Python worker to supply `charge`.
                String id = client.start(blueprint, Purchase.of("A-1001", 2));
                System.out.println("\n>>> waiting for the Python payments worker to implement `charge` ...");
                System.out.println(">>> run:  python clients/python/examples/polyglot_typed_worker.py\n");

                InstanceView view = client.awaitCompletion(id, Duration.ofMinutes(2));
                Purchase result = codec.decode(view.context());   // typed again on the way out
                System.out.println("[result] status=" + view.status()
                        + " -> Purchase{orderId=" + result.orderId()
                        + ", status=" + result.status()
                        + ", paymentRef=" + result.paymentRef() + "}");
            }
        }
    }
}