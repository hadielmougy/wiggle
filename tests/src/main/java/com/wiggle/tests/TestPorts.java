package com.wiggle.tests;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A port for a test that has to know the number <em>before</em> the server binds it.
 *
 * <p>The obvious way to get one is to open a {@code ServerSocket(0)}, read the port the OS picked,
 * and close it again. That is a trap, and it produced a real intermittent failure in this suite: the
 * number the OS hands back comes from the <b>ephemeral range</b> (49152-65535 on macOS, 32768-60999
 * on Linux), which is the very pool it draws from to satisfy every other {@code port 0} bind in the
 * process. Between the close and the eventual bind, that number is free for the taking, and the JVM
 * running this suite starts hundreds of gRPC servers on {@code port 0}.
 *
 * <p>When the two collided the symptom was nowhere near the cause: a gRPC client dialling what it had
 * been told was its own server's port got an HTTP/1.1 response from the {@code /healthz} endpoint, and
 * Netty reported {@code First received frame was not SETTINGS. Hex dump for first 5 bytes: 485454502f}
 * -- {@code "HTTP/"} -- which gRPC surfaces as {@code INTERNAL: http2 exception} in whichever
 * unrelated test happened to be running.
 *
 * <p>So this allocates below the ephemeral range instead. Nothing is ever assigned these numbers
 * spontaneously; only another explicit reservation could want one, and the bind check below catches
 * that. The window between the check and the caller's bind still exists -- it is unavoidable without
 * handing the open socket over -- but it is no longer a window onto a pool the OS is actively
 * handing out.
 */
public final class TestPorts {

    private TestPorts() {}

    /** Below the ephemeral range on both macOS (49152+) and Linux (32768+). */
    private static final int LOW = 20_000;
    private static final int HIGH = 32_000;

    /** Seeded from the PID so that modules tested in parallel JVMs do not walk the same sequence. */
    private static final AtomicInteger NEXT =
            new AtomicInteger(Math.floorMod((int) ProcessHandle.current().pid() * 2_654_435_761L, HIGH - LOW));

    /**
     * A port outside the ephemeral range that is bindable right now. Retries past any that is already
     * taken, so two suites sharing a machine do not collide.
     */
    public static int free() {
        for (int attempt = 0; attempt < 500; attempt++) {
            int port = LOW + Math.floorMod(NEXT.getAndIncrement(), HIGH - LOW);
            try (ServerSocket probe = new ServerSocket(port)) {
                return probe.getLocalPort();
            } catch (IOException taken) {
                // in use by something else -- walk on
            }
        }
        throw new IllegalStateException("no free port in [" + LOW + "," + HIGH + ") after 500 tries");
    }
}
