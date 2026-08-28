package dev.wiggle.binding.typed;

import dev.wiggle.client.WiggleClient;
import dev.wiggle.client.worker.Worker;
import dev.wiggle.core.ContextCodec;
import dev.wiggle.core.InstanceView;
import dev.wiggle.server.ServerConfig;
import dev.wiggle.server.WiggleServer;

import java.time.Duration;

/**
 * Name-only binding with a <b>typed</b> context. The flow is authored once as a {@link Purchase}
 * record (via {@link ContextCodec#records}), and workers implement its steps by name with typed
 * handlers ({@code Purchase -> Purchase}) -- no worker re-declares the graph. This process:
 * <ol>
 *   <li>starts an embedded server on :8080 and registers the {@code typed-order} graph;</li>
 *   <li>runs a worker that binds every step but {@code charge} by name, typed;</li>
 *   <li>runs a separate payments worker that owns {@code charge} on its own queue;</li>
 *   <li>submits a {@link Purchase} and prints the (typed) result.</li>
 * </ol>
 * The record is just JSON on the wire, so the same step could equally be served by a worker in
 * another language that treats the context as a plain map -- interop is by activity name, not type.
 */
public final class TypedBindingDemo {

    public static void main(String[] args) throws Exception {
        ServerConfig config = new ServerConfig(8080, "typed-binding-node", null, null, null, 4,
                Duration.ofMillis(200), Duration.ofSeconds(2), 3, Duration.ofSeconds(30),
                Duration.ofSeconds(2), Duration.ofHours(1), 100, 0, Duration.ofSeconds(5), Duration.ofSeconds(10));

        ContextCodec<Purchase> codec = TypedBindingOrder.codec();

        try (WiggleServer server = new WiggleServer(config).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {

            // (1) author: register the topology, once.
            var blueprint = TypedBindingOrder.blueprint();
            client.register(blueprint);
            System.out.println("[author] registered " + blueprint.name() + " v" + blueprint.version());

            // (2) fulfilment worker: typed handlers for everything but payments.
            // (3) payments worker: owns only `charge`, on its own queue.
            try (Worker fulfilment = new Worker(client, "typed-fulfilment");
                 Worker payments = new Worker(client, "typed-payments")) {

                fulfilment.handle(TypedBindingOrder.NAME, "validate", codec, p -> p.withStatus("VALIDATED"))
                          .handleGate(TypedBindingOrder.NAME, "in-stock", codec, p -> p.quantity() > 0)
                          .handleEffect(TypedBindingOrder.NAME, "notify", codec,
                                  p -> System.out.println("   [fulfilment] notified " + p.orderId()
                                          + " status=" + p.status() + " payment=" + p.paymentRef()))
                          .start();
                System.out.println("[fulfilment] serving validate / in-stock / notify as typed Purchase handlers");

                payments.handle(TypedBindingOrder.NAME, "charge", codec,
                                p -> p.withPaymentRef("auth-" + p.orderId()))
                        .start();
                System.out.println("[payments]   serving charge on the payments queue");

                String id = client.start(blueprint, Purchase.of("A-1001", 2));
                InstanceView view = client.awaitCompletion(id, Duration.ofSeconds(30));
                Purchase result = codec.decode(view.context());   // typed again on the way out
                System.out.println("\n[result] status=" + view.status()
                        + " -> Purchase{orderId=" + result.orderId()
                        + ", status=" + result.status()
                        + ", paymentRef=" + result.paymentRef() + "}");
            }
        }
    }
}
