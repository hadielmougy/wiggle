package dev.wiggle.tests;

import dev.wiggle.client.WiggleClient;
import dev.wiggle.client.dsl.Blueprint;
import dev.wiggle.client.dsl.Workflow;
import dev.wiggle.server.ServerConfig;
import dev.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Cold-start regression: a worker-critical call (here {@code register}) issued <em>before</em> the
 * server exists must WAIT for it -- via the wait-for-ready stub -- and then succeed once it comes up,
 * rather than failing fast with {@code UNAVAILABLE}. This is what lets a fleet of workers ride out a
 * cold start / rolling restart / reschedule without crashing.
 */
class ColdStartTest {

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    @Test
    @DisplayName("register issued before the server is up waits for it, then succeeds")
    void registerWaitsForColdServer() throws Exception {
        int port = freePort();
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try (WiggleClient client = new WiggleClient("localhost:" + port)) {
            Blueprint<Map<String, Object>> bp = Workflow.define("cold").step("a", c -> c).build();

            // Register in the background; nothing is listening yet, so it must block (not fail fast).
            Future<?> future = pool.submit(() -> {
                client.register(bp);
                return null;
            });
            Thread.sleep(600);
            assertFalse(future.isDone(), "register returned before the server was up (fail-fast, not wait-for-ready)");

            // Bring the server up on that port; the blocked register must now complete.
            ServerConfig config = new ServerConfig(port, "cold-node", null, null, null, 4,
                    Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                    Duration.ofMillis(500), Duration.ofHours(1), 100, 0, Duration.ofSeconds(5), Duration.ofSeconds(10));
            try (WiggleServer server = new WiggleServer(config).start()) {
                future.get(10, TimeUnit.SECONDS); // no exception == it waited and succeeded
            }
        } finally {
            pool.shutdownNow();
        }
    }
}