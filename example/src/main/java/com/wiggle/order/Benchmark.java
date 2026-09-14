package com.wiggle.order;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.WiggleClient;
import com.wiggle.client.worker.Worker;
import com.wiggle.client.worker.WorkerOptions;
import com.wiggle.core.ExecutionMode;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import com.wiggle.client.flow.FlowFn;
import com.wiggle.client.flow.WiggleFlow;

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
        FlowSpec bp = linear("bench-linear", steps, mode);

        ServerConfig config = new ServerConfig(0, "bench", jdbcUrl, jdbcUser, jdbcPassword, 16,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(30),
                Duration.ofMillis(200), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));

        try (WiggleServer server = open(config, jdbcUrl);
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            client.register(bp);

            // Pre-submit everything; workers aren't running yet, so it all queues.
            for (int i = 0; i < count; i++) client.start(bp, Map.of("i", (long) i));

            List<Worker> pool = new ArrayList<>();
            long t0 = System.nanoTime();
            for (int i = 0; i < workers; i++) {
                pool.add(new Worker(client, "bench-worker-" + i, WorkerOptions.defaults()
                        .withConcurrency(concurrency).withLocalBatchSize(batch))
                        .handlers(new BenchHandlers(done)).start());
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

    /** In-memory with no URL; else JdbcStorage with the dialect picked from the URL (postgres / h2).
     *  Storage is an explicit factory (no ServiceLoader), so the benchmark wires it here. */
    private static WiggleServer open(ServerConfig config, String jdbcUrl) throws Exception {
        if (jdbcUrl == null) return new WiggleServer(config).start();
        com.wiggle.jdbc.Dialect dialect = jdbcUrl.startsWith("jdbc:postgresql")
                ? new com.wiggle.postgres.PostgresDialect() : new com.wiggle.postgres.H2Dialect();
        return new WiggleServer(config, cfg -> new com.wiggle.jdbc.JdbcStorage(
                cfg.jdbcUrl(), cfg.jdbcUser(), cfg.jdbcPassword(), cfg.jdbcPoolSize(), dialect)).start();
    }

    /** The longest pipeline this benchmark can build -- one declared step method per hop. */
    public static final int MAX_STEPS = 32;

    /**
     * The chain's steps, declared. A step is named by referencing a method, so the pipeline's length
     * is bounded by how many are declared here rather than being generated -- hence {@link #MAX_STEPS}.
     * They are all the same identity hop; the distinct names exist only so each is its own node.
     */
    public interface BenchSteps {
        Map<String, Object> hop1(Map<String, Object> ctx);
        Map<String, Object> hop2(Map<String, Object> ctx);
        Map<String, Object> hop3(Map<String, Object> ctx);
        Map<String, Object> hop4(Map<String, Object> ctx);
        Map<String, Object> hop5(Map<String, Object> ctx);
        Map<String, Object> hop6(Map<String, Object> ctx);
        Map<String, Object> hop7(Map<String, Object> ctx);
        Map<String, Object> hop8(Map<String, Object> ctx);
        Map<String, Object> hop9(Map<String, Object> ctx);
        Map<String, Object> hop10(Map<String, Object> ctx);
        Map<String, Object> hop11(Map<String, Object> ctx);
        Map<String, Object> hop12(Map<String, Object> ctx);
        Map<String, Object> hop13(Map<String, Object> ctx);
        Map<String, Object> hop14(Map<String, Object> ctx);
        Map<String, Object> hop15(Map<String, Object> ctx);
        Map<String, Object> hop16(Map<String, Object> ctx);
        Map<String, Object> hop17(Map<String, Object> ctx);
        Map<String, Object> hop18(Map<String, Object> ctx);
        Map<String, Object> hop19(Map<String, Object> ctx);
        Map<String, Object> hop20(Map<String, Object> ctx);
        Map<String, Object> hop21(Map<String, Object> ctx);
        Map<String, Object> hop22(Map<String, Object> ctx);
        Map<String, Object> hop23(Map<String, Object> ctx);
        Map<String, Object> hop24(Map<String, Object> ctx);
        Map<String, Object> hop25(Map<String, Object> ctx);
        Map<String, Object> hop26(Map<String, Object> ctx);
        Map<String, Object> hop27(Map<String, Object> ctx);
        Map<String, Object> hop28(Map<String, Object> ctx);
        Map<String, Object> hop29(Map<String, Object> ctx);
        Map<String, Object> hop30(Map<String, Object> ctx);
        Map<String, Object> hop31(Map<String, Object> ctx);
        Map<String, Object> hop32(Map<String, Object> ctx);
        Map<String, Object> sink(Map<String, Object> ctx);
    }

    /**
     * A linear chain of {@code steps} trivial same-queue task steps ending in a {@code sink} step.
     * The chain is pure topology; the logic lives in {@link BenchHandlers}. Every step but the last is
     * an identity hop, so the pipeline measures dispatch and commit rather than step work; the final
     * {@code sink} counts the instance down exactly once.
     */
    private static FlowSpec linear(String name, int steps, ExecutionMode mode) {
        if (steps < 1 || steps > MAX_STEPS) {
            throw new IllegalArgumentException("WIGGLE_BENCH_STEPS must be 1.." + MAX_STEPS + ", got " + steps);
        }
        return FlowSpec.define(name, Map.class, BenchSteps.class, (f, s) -> {
            List<FlowFn<Map, Map>> hops = hops(s);
            WiggleFlow<Map> chain = f.execution(mode);
            for (int i = 0; i < steps - 1; i++) {
                chain = chain.thenApply(hops.get(i));
            }
            return chain.thenApply(s::sink);
        });
    }

    /** The declared hops in order, so the chain can take the first {@code steps - 1} of them. */
    private static List<FlowFn<Map, Map>> hops(BenchSteps s) {
        return List.of(
                s::hop1,
                s::hop2,
                s::hop3,
                s::hop4,
                s::hop5,
                s::hop6,
                s::hop7,
                s::hop8,
                s::hop9,
                s::hop10,
                s::hop11,
                s::hop12,
                s::hop13,
                s::hop14,
                s::hop15,
                s::hop16,
                s::hop17,
                s::hop18,
                s::hop19,
                s::hop20,
                s::hop21,
                s::hop22,
                s::hop23,
                s::hop24,
                s::hop25,
                s::hop26,
                s::hop27,
                s::hop28,
                s::hop29,
                s::hop30,
                s::hop31,
                s::hop32);
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? def : v;
    }

    private static int intEnv(String key, int def) {
        return Integer.parseInt(env(key, Integer.toString(def)));
    }
}
