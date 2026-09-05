package com.wiggle.greet;

import com.wiggle.client.WiggleConnection;
import com.wiggle.client.dsl.Blueprint;
import com.wiggle.core.Tls;

import java.util.Map;

/**
 * Starts one greet instance through the coordinator. The client is coordinator-aware: it resolves the
 * namespace to its owning cell ({@code clientForNamespace}) and dials that cell -- the caller never
 * needs to know which cell. It first allocates the definition to the namespace (idempotent), so the
 * start succeeds whether or not a worker has registered it yet. The returned id
 * ({@code ns1.e0.s0.<ulid>}) is self-routing: resolve it later with {@code clientForInstance}.
 *
 * <pre>
 *   WIGGLE_COORDINATOR_URL=127.0.0.1:8099 WIGGLE_NAMESPACE=ns1 \
 *     java -cp 'example/build/install/example/lib/*' com.wiggle.greet.GreetStart ada
 * </pre>
 */
public final class GreetStart {

    public static void main(String[] args) throws Exception {
        String coord = env("WIGGLE_COORDINATOR_URL", "127.0.0.1:8099");
        String ns = env("WIGGLE_NAMESPACE", "ns1");
        String name = args.length > 0 ? args[0] : "ada";

        try (var resolver = WiggleConnection.coordinator(coord, Tls.Options.DISABLED, null)) {
            Blueprint bp = GreetFlow.blueprint();
            resolver.registerWorkflow(ns, bp);   // allocate the definition to the namespace's cells (idempotent)
            String instanceId = resolver.clientForNamespace(ns).start(bp, Map.<String, Object>of("name", name));
            System.out.println("started " + bp.name() + " in namespace '" + ns + "' -> " + instanceId);
            System.out.println("  self-routing id; act on it later via resolver.clientForInstance(id)");
        }
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? def : v;
    }
}
