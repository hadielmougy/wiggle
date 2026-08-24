package dev.wiggle.tests;

import dev.wiggle.client.dsl.Blueprint;
import dev.wiggle.client.dsl.Workflow;
import dev.wiggle.client.worker.PollResult;
import dev.wiggle.client.worker.WiggleClient;
import dev.wiggle.core.Ids;
import dev.wiggle.core.Tls;
import dev.wiggle.server.ServerConfig;
import dev.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end load shedding driven by the gRPC handler thread-pool queue. With a small bounded pool,
 * a burst of concurrent long-polls saturates it; the server then stops long-polling and hands the
 * waiting workers a jittered hold-off (empty + {@code retryAfterMillis}) instead of parking them all
 * and blocking forever. The point: under saturation the polls come back promptly with a back-off,
 * they don't hang.
 */
class StabilityPollTest {

    private static ServerConfig config() {
        ServerConfig.Stability stability = new ServerConfig.Stability(
                true, /*threads*/ 2, /*high*/ 2, /*low*/ 0,
                Duration.ofMillis(20), /*holdOff*/ Duration.ofMillis(200), /*jitter*/ Duration.ofMillis(100));
        return new ServerConfig(0, "stab-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                /*maxLongPoll*/ Duration.ofMillis(800), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10), "admin", null, Tls.Options.DISABLED, stability);
    }

    @Test @DisplayName("a burst of long-polls saturates the pool, so the server sheds with hold-offs instead of blocking")
    void shedsUnderThreadPoolSaturation() throws Exception {
        Blueprint<Map<String, Object>> bp = Workflow.defineJson("stab-" + Ids.next("wf"))
                .step("work", ctx -> ctx)
                .build();
        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            client.register(bp);   // registered, but no instances -> every poll is empty and would long-poll

            int workers = 12;      // >> the 2 handler threads, so requests queue up and shedding kicks in
            ExecutorService pool = Executors.newFixedThreadPool(workers);
            List<Future<PollResult>> futures = new java.util.ArrayList<>();
            for (int i = 0; i < workers; i++) {
                String w = "w-" + i;
                futures.add(pool.submit(() -> client.poll(w, Set.of("q-idle"), 1, 30_000, 5_000)));
            }

            int gotHoldOff = 0;
            for (Future<PollResult> f : futures) {
                PollResult r = f.get(15, java.util.concurrent.TimeUnit.SECONDS);   // must NOT hang
                assertTrue(r.tasks().isEmpty(), "no work exists, so every poll is empty");
                if (r.retryAfterMillis() > 0) {
                    assertTrue(r.retryAfterMillis() >= 200 && r.retryAfterMillis() <= 300,
                            "hold-off carries jitter within [base, base+jitter]: " + r.retryAfterMillis());
                    gotHoldOff++;
                }
            }
            pool.shutdown();
            assertTrue(gotHoldOff > 0, "under saturation the server sheds: at least one poll got a hold-off hint");
        }
    }
}
