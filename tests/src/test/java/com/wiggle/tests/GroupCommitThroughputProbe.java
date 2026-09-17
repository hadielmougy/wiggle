package com.wiggle.tests;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.ForFlow;
import com.wiggle.client.worker.Worker;
import com.wiggle.client.worker.WorkerOptions;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Directional throughput probe for WIGGLE_GROUP_COMMIT, not an assertion -- it prints steps/sec and
 * only asserts completion. Run it twice and compare:
 *
 * <pre>
 *   WIGGLE_BENCH_GROUP_COMMIT=1                        ./gradlew :tests:test --tests '*GroupCommitThroughputProbe*'
 *   WIGGLE_BENCH_GROUP_COMMIT=1 WIGGLE_GROUP_COMMIT=true ./gradlew :tests:test --tests '*GroupCommitThroughputProbe*'
 * </pre>
 *
 * With WIGGLE_TEST_DB_URL set it measures against that database (where the commit cost lives);
 * without, against per-run H2.
 */
@EnabledIfEnvironmentVariable(named = "WIGGLE_BENCH_GROUP_COMMIT", matches = ".+")
class GroupCommitThroughputProbe {

    interface Steps {
        Map<String, Object> a(Map<String, Object> c);
        Map<String, Object> b(Map<String, Object> c);
        Map<String, Object> c(Map<String, Object> c);
    }

    @ForFlow("gc-probe")
    static final class H {
        public Map<String, Object> a(Map<String, Object> c) { return c; }
        public Map<String, Object> b(Map<String, Object> c) { return c; }
        public Map<String, Object> c(Map<String, Object> c) { return c; }
    }

    private static final int WARMUP = 50;
    private static final int MEASURED = 300;
    private static final int STEPS = 3;

    @Test @Timeout(300)
    void measure() throws Exception {
        FlowSpec spec = FlowSpec.define("gc-probe", Map.class, Steps.class,
                (f, s) -> f.thenApply(s::a).thenApply(s::b).thenApply(s::c));
        ServerConfig config = new ServerConfig(
                0, "gc-probe-node", TestStorage.url("gcprobe"), TestStorage.user(), TestStorage.password(), 8,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0, Duration.ofSeconds(5), Duration.ofSeconds(10));

        try (WiggleServer server = new WiggleServer(config, new com.wiggle.dist.WiggleStorageFactory()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            client.register(spec);
            try (Worker w = new Worker(client, "gc-probe-w",
                    WorkerOptions.defaults().withConcurrency(64).withLongPollWait(Duration.ofMillis(200)))
                    .registerHandler(new H()).start()) {

                runBatch(client, spec, WARMUP);

                long t0 = System.nanoTime();
                runBatch(client, spec, MEASURED);
                double secs = (System.nanoTime() - t0) / 1e9;

                String mode = System.getenv().getOrDefault("WIGGLE_GROUP_COMMIT", "false");
                String db = TestStorage.isLive() ? "live-db" : "h2";
                System.out.printf("[gc-probe] groupCommit=%s db=%s instances=%d steps=%d elapsed=%.2fs -> %.0f steps/s%n",
                        mode, db, MEASURED, MEASURED * STEPS, secs, MEASURED * STEPS / secs);
            }
        }
    }

    private static void runBatch(WiggleClient client, FlowSpec spec, int n) {
        List<String> ids = new ArrayList<>(n);
        for (int i = 0; i < n; i++) ids.add(client.start(spec, Map.of("i", i)));
        for (String id : ids) client.awaitCompletion(id, Duration.ofSeconds(120));
    }
}
