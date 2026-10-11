package com.wiggle.testfixtures;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Ports for test servers, which test clients dial on {@code 127.0.0.1}.
 *
 * <p>A server listens on the wildcard address with {@code SO_REUSEADDR}, and on macOS that bind
 * succeeds even when another process holds {@code 127.0.0.1} on the same port; connections to
 * {@code 127.0.0.1} then reach that process, not the test's server. The client sees
 * {@code http2 exception} when the other process speaks HTTP, {@code Network closed for unknown
 * reason} when it drops the connection. A port is therefore handed out only when it can be bound on
 * {@code 127.0.0.1} and on the wildcard address with {@code SO_REUSEADDR} off. Never start a test
 * server on port 0: the ephemeral range it draws from is where IDEs, Docker and other local tools
 * hold loopback ports.
 */
public final class TestPorts {

    private TestPorts() {}

    /** Below the ephemeral range on both macOS (49152+) and Linux (32768+). */
    private static final int LOW = 20_000;
    private static final int HIGH = 32_000;

    /** Seeded from the PID so that modules tested in parallel JVMs do not walk the same sequence. */
    private static final AtomicInteger NEXT =
            new AtomicInteger(Math.floorMod((int) ProcessHandle.current().pid() * 2_654_435_761L, HIGH - LOW));

    /** A port in [{@value #LOW}, {@value #HIGH}) that nothing holds right now, on loopback or anywhere. */
    public static int free() {
        for (int attempt = 0; attempt < 500; attempt++) {
            int port = LOW + Math.floorMod(NEXT.getAndIncrement(), HIGH - LOW);
            if (bindable(port)) return port;
        }
        throw new IllegalStateException("no free port in [" + LOW + "," + HIGH + ") after 500 tries");
    }

    /** Whether {@code port} binds on {@code 127.0.0.1} and on the wildcard address, without reuse. */
    static boolean bindable(int port) {
        return bindable(new InetSocketAddress(InetAddress.getLoopbackAddress(), port))
                && bindable(new InetSocketAddress(port));
    }

    private static boolean bindable(InetSocketAddress at) {
        try (ServerSocket probe = new ServerSocket()) {
            probe.setReuseAddress(false);
            probe.bind(at);
            return true;
        } catch (IOException taken) {
            return false;
        }
    }
}
