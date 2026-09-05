package com.wiggle.order;

import com.wiggle.client.WiggleConnection;
import com.wiggle.client.dsl.Blueprint;
import com.wiggle.core.InstanceView;
import com.wiggle.core.Tls;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Submits OrderFulfilment instances through the coordinator. The client is namespace-aware: it
 * resolves the namespace to its owning cell ({@code clientForNamespace}) and dials that cell -- the
 * caller never needs to know which cell. It first allocates the definition to the namespace
 * (idempotent), so the start succeeds whether or not a worker has registered it yet. Each returned
 * id is self-routing, so completion is awaited via {@code clientForInstance(id)}.
 *
 * <pre>
 *   WIGGLE_COORDINATOR_URL=127.0.0.1:18099 WIGGLE_NAMESPACE=abc \
 *     java -cp 'example/build/install/example/lib/*' com.wiggle.order.NamespaceSubmitter 5
 * </pre>
 */
public final class NamespaceSubmitter {

    public static void main(String[] args) throws Exception {
        String coord = env("WIGGLE_COORDINATOR_URL", "127.0.0.1:18099");
        String ns = env("WIGGLE_NAMESPACE", "abc");
        int count = args.length > 0 ? Integer.parseInt(args[0]) : 100;

        try (WiggleConnection resolver = WiggleConnection.coordinator(coord, Tls.Options.DISABLED, "us")) {
            Blueprint bp = OrderFulfilment.blueprint();
            resolver.registerWorkflow(ns, bp);   // allocate the definition to the namespace's cells (idempotent)
            // Cell addresses come from the coordinator as in-cluster pod IPs; to reach them from the host
            // AND spread starts across cells, set WIGGLE_ENDPOINT_REWRITE (each cell's pod IP -> its own
            // port-forward). WiggleConnection picks it up from the env by default -- the lab's Forwards tab
            // generates the exact value. Without it, every start would land on one forwarded cell.

            List<String> ids = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                Order order = Order.of("A-" + (2000 + i), "customer-" + i, 1 + (i % 3),
                        new BigDecimal("100.00").add(BigDecimal.valueOf(i)));
                ids.add(resolver.clientForNamespace(ns).start(bp, order));
                Thread.sleep(10);
            }
            System.out.println("submitted " + count + " orders to namespace '" + ns + "' via coordinator " + coord);

            for (String id : ids) {
                InstanceView v = resolver.clientForInstance(id).awaitCompletion(id, Duration.ofMinutes(2));
                System.out.println("  " + id + "  " + v.status()
                        + (v.error() == null ? "" : "  " + v.error()));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? def : v;
    }
}
