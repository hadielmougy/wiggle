package dev.wiggle.order;

import dev.wiggle.client.dsl.Blueprint;
import dev.wiggle.client.dsl.Workflow;
import dev.wiggle.client.dsl.WorkflowStream;
import dev.wiggle.client.worker.WiggleClient;
import dev.wiggle.client.worker.Worker;
import dev.wiggle.client.worker.WorkerOptions;
import dev.wiggle.core.ExecutionMode;
import dev.wiggle.server.ServerConfig;
import dev.wiggle.server.WiggleServer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * A throughput micro-benchmark for the execution modes on the case that actually exercises
 * batching: a long <em>linear</em> pipeline of same-queue steps with no boundaries in the middle
 * (no fork / sleep / join / user task), so LOCAL_ASYNC collapses the whole run's status writes
 * into one call while SERVER/LOCAL_SYNC pay per step.
 *
 * <p>Instances are pre-submitted (they queue as READY) and only then are workers started, so the
 * timing isolates drain throughput from submission. All in one JVM (in-memory store) to remove
 * network and DB from the picture and measure the engine/transport overhead the mode controls.
 *
 * <pre>
 *   WIGGLE_EXECUTION_MODE=SERVER      ./gradlew :example:bench
 *   WIGGLE_EXECUTION_MODE=LOCAL_SYNC  ./gradlew :example:bench
 *   WIGGLE_EXECUTION_MODE=LOCAL_ASYNC WIGGLE_LOCAL_BATCH_SIZE=64 ./gradlew :example:bench
 * </pre>
 * Tunables: {@code WIGGLE_BENCH_STEPS} (20), {@code WIGGLE_BENCH_COUNT} (2000),
 * {@code WIGGLE_BENCH_WORKERS} (4), {@code WIGGLE_WORKER_CONCURRENCY} (16).
 */
public final class Benchmark {

    public static void main(String[] args) throws Exception {
        ExecutionMode mode = ExecutionMode.valueOf(env("WIGGLE_EXECUTION_MODE", "SERVER"));
        int steps = intEnv("WIGGLE_BENCH_STEPS", 20);
        int count = intEnv("WIGGLE_BENCH_COUNT", 2000);
        int workers = intEnv("WIGGLE_BENCH_WORKERS", 4);
        int concurrency = intEnv("WIGGLE_WORKER_CONCURRENCY", 16);
        int batch = intEnv("WIGGLE_LOCAL_BATCH_SIZE", 64);

        // Point at a real database (WIGGLE_JDBC_URL) to see LOCAL_ASYNC's commit-batching win;
        // with no URL it uses the in-memory store, where commits are ~free so async ~= sync.
        String jdbcUrl = env("WIGGLE_JDBC_URL", null);
        String jdbcUser = env("WIGGLE_JDBC_USER", null);
        String jdbcPassword = env("WIGGLE_JDBC_PASSWORD", null);

        CountDownLatch done = new CountDownLatch(count);
        Blueprint<Map<String, Object>> bp = linear("bench-linear", steps, mode, done);

        ServerConfig config = new ServerConfig(0, "bench", jdbcUrl, jdbcUser, jdbcPassword, 16,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(30),
                Duration.ofMillis(200), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));

        try (WiggleServer server = new WiggleServer(config).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            client.register(bp);

            // Pre-submit everything; workers aren't running yet, so it all queues.
            for (int i = 0; i < count; i++) client.start(bp, Map.of("i", (long) i));

            List<Worker> pool = new ArrayList<>();
            long t0 = System.nanoTime();
            for (int i = 0; i < workers; i++) {
                pool.add(new Worker(client, "bench-worker-" + i, WorkerOptions.defaults()
                        .withConcurrency(concurrency).withLocalBatchSize(batch)).register(bp).start());
            }
            done.await();
            long t1 = System.nanoTime();
            for (Worker w : pool) w.close();

            double seconds = (t1 - t0) / 1e9;
            System.out.printf(
                    "mode=%-11s storage=%-9s steps=%d instances=%d workers=%d concurrency=%d batch=%d "
                            + "=> %.0f instances/sec (%.2fs, %.0f step-completions/sec)%n",
                    mode, jdbcUrl == null ? "in-memory" : "jdbc", steps, count, workers, concurrency, batch,
                    count / seconds, seconds, (long) count * steps / seconds);
        }
    }

    /** A linear chain of {@code steps} trivial same-queue map steps; the last one counts down. */
    private static Blueprint<Map<String, Object>> linear(String name, int steps, ExecutionMode mode, CountDownLatch done) {
        WorkflowStream<Map<String, Object>> s = Workflow.defineJson(name).execution(mode);
        for (int i = 0; i < steps; i++) {
            boolean last = i == steps - 1;
            s = s.step("s" + i, last ? ctx -> { done.countDown(); return ctx; } : ctx -> ctx);
        }
        return s.build();
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? def : v;
    }

    private static int intEnv(String key, int def) {
        return Integer.parseInt(env(key, Integer.toString(def)));
    }
}
