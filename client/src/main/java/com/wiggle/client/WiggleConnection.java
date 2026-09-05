package com.wiggle.client;

import com.wiggle.core.Tls;

/**
 * The single client-side entry point for reaching Wiggle. Pick a mode; each factory returns a type
 * that exposes only the operations valid for it -- so illegal calls are a compile error, not a
 * runtime throw:
 * <ul>
 *   <li>{@link #direct(String) direct} -> a {@link DirectConnection} to one standalone server;
 *       {@link DirectConnection#client()} is the single connection, no namespaces, no routing.</li>
 *   <li>{@link #coordinator coordinator} -> a {@link CoordinatedConnection} that routes to the cell
 *       owning a namespace or instance and carries the coordinator-only admin operations
 *       (register / open-epoch / list).</li>
 * </ul>
 */
public final class WiggleConnection {

    private WiggleConnection() {}

    /** One standalone server, no TLS. */
    public static DirectConnection direct(String target) {
        return new DirectConnection(target, Tls.Options.DISABLED);
    }

    /** One standalone server. */
    public static DirectConnection direct(String target, Tls.Options tls) {
        return new DirectConnection(target, tls);
    }

    /** A sharded namespace, routed through the coordinator at {@code coordinatorUrl}. */
    public static CoordinatedConnection coordinator(String coordinatorUrl, Tls.Options tls, String callerRegion) {
        return new CoordinatedConnection(coordinatorUrl, tls, callerRegion);
    }

    /** Strip any {@code scheme://} prefix from a target (shared by both connection types). */
    static String strip(String target) {
        if (target == null) return null;
        int i = target.indexOf("://");
        return i < 0 ? target : target.substring(i + 3);
    }
}
