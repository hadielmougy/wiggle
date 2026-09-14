package com.wiggle.tests;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.core.Ids;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * {@code close()} has to actually close.
 *
 * <p>A worker parked in a long poll is an in-flight RPC, and a graceful shutdown waits for those. The
 * wait cannot be unbounded, so it is capped -- but when the cap expires the server has to be taken
 * down rather than left running: a half-closed server keeps its listening port, its executor, and its
 * share of the Netty event-loop groups gRPC shares process-wide. One is harmless. A suite that starts
 * hundreds of servers in a single JVM accumulates them, and that is the kind of debris that surfaces
 * later as an unexplained transport failure in whichever test happens to run next.
 *
 * <p>So this parks a long poll, closes, and requires the port to stop accepting connections.
 */
class ServerShutdownTest {

    /** The step this spec names; a worker binds it by name. */
    interface OneStep {
        Map<String, Object> work(Map<String, Object> ctx);
    }

    private static ServerConfig config() {
        return new ServerConfig(TestPorts.free(), "shutdown-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                // maxLongPoll 30s: comfortably longer than close()'s graceful wait, so the poll is
                // still parked when the wait expires -- the case that used to leak the server.
                Duration.ofSeconds(30), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    @Test
    @DisplayName("closing a server whose worker is parked in a long poll still frees the port")
    void closeTerminatesEvenWithAnInFlightLongPoll() throws Exception {
        FlowSpec spec = FlowSpec.define("shutdown-" + Ids.next("wf"), Map.class, OneStep.class, (f, s) -> f.thenApply(s::work));

        WiggleServer server = new WiggleServer(config()).start();
        int port = server.port();
        WiggleClient client = new WiggleClient(server.baseUrl());
        client.register(spec);

        // Park a real long poll on the server: no task is ever produced, so it holds for the full 30s.
        CompletableFuture<?> parked = CompletableFuture.supplyAsync(() -> {
            try {
                return client.poll("parked-worker", spec.definition().workerQueues(), 1, 30_000, 30_000);
            } catch (RuntimeException e) {
                return null;    // the close tears this poll down; that is the point, not a failure
            }
        });
        Thread.sleep(500);      // let the poll reach the server and block there

        server.close();
        client.close();

        assertFalse(accepting(port),
                "close() returned while the server was still listening on " + port
                        + " -- a server that outlives its own close() keeps the port and the shared "
                        + "event-loop groups, and nothing will ever take it down");

        parked.cancel(true);
    }

    /** Whether anything still accepts a TCP connection on {@code port}. */
    private static boolean accepting(int port) {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress("127.0.0.1", port), 500);
            return true;
        } catch (ConnectException e) {
            return false;
        } catch (Exception e) {
            return false;
        }
    }
}
