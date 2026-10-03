package com.wiggle.client;

import com.wiggle.core.Tls;

/**
 * The client-side entry point for reaching Wiggle: {@link #direct(String) direct} returns a
 * {@link DirectConnection} to a cluster, whose {@link DirectConnection#client()} is the single
 * connection.
 */
public final class WiggleConnection {

    private WiggleConnection() {}

    /** One cluster, no TLS. */
    public static DirectConnection direct(String target) {
        return direct(target, Tls.Options.DISABLED);
    }

    /** One cluster. */
    public static DirectConnection direct(String target, Tls.Options tls) {
        return new DirectConnection(target, tls);
    }

    /** Strip any {@code scheme://} prefix from a target. */
    static String strip(String target) {
        if (target == null) return null;
        int i = target.indexOf("://");
        return i < 0 ? target : target.substring(i + 3);
    }
}
