package com.wiggle.order;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.client.worker.Handlers;
import com.wiggle.client.worker.Worker;
import com.wiggle.client.worker.WorkerOptions;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * A timer-throughput micro-benchmark: every instance parks on a server-side {@code sleep}, so the
 * drain rate is gated by how fast the housekeeper promotes due timers — {@code batch} per tick with
 * the fixed sweep, batch-until-partial with adaptive housekeeping
 * ({@code WIGGLE_ADAPTIVE_HOUSEKEEPING=true}).
 *
 * <p>All instances are pre-submitted and the workers started; every instance runs
 * {@code enter → sleep(short) → exit}, so within a moment the whole population's timers are due at
 * once — the worst-case promotion burst. With the production-default cadence (1s tick × batch 100)
 * a fixed sweep needs {@code count/batch} ticks; the adaptive sweep drains the backlog in the first
 * tick after it becomes due.
 *
 * <pre>
 *   ./gradlew :example:timerBench                                     # fixed (baseline)
 *   WIGGLE_ADAPTIVE_HOUSEKEEPING=true ./gradlew :example:timerBench   # adaptive
 * </pre>
 * Tunables: {@code WIGGLE_BENCH_COUNT} (2000), {@code WIGGLE_BENCH_SLEEP_MILLIS} (25),
 * {@code WIGGLE_TICK_MILLIS} (1000), {@code WIGGLE_HOUSEKEEPING_BATCH} (100),
 * {@code WIGGLE_BENCH_WORKERS} (4), {@code WIGGLE_WORKER_CONCURRENCY} (16).
 */
public final class TimerBench {

    public static void main(String[] args) throws Exception {
        int count = intEnv("WIGGLE_BENCH_COUNT", 2000);
        long sleepMillis = intEnv("WIGGLE_BENCH_SLEEP_MILLIS", 25);
        long tickMillis = intEnv("WIGGLE_TICK_MILLIS", 1000);
        int batch = intEnv("WIGGLE_HOUSEKEEPING_BATCH", 100);
        int workers = intEnv("WIGGLE_BENCH_WORKERS", 4);
        int concurrency = intEnv("WIGGLE_WORKER_CONCURRENCY", 16);
        boolean adaptive = Boolean.parseBoolean(env("WIGGLE_ADAPTIVE_HOUSEKEEPING", "false"));

        CountDownLatch done = new CountDownLatch(count);
        Blueprint bp = Workflow.define("bench-timer")
                .step("enter")
                .sleep("hold", Duration.ofMillis(sleepMillis))
                .effect("exit")
                .build();

        ServerConfig config = new ServerConfig(0, "timer-bench", null, null, null, 16,
                Duration.ofMillis(tickMillis), Duration.ofMillis(500), 3, Duration.ofSeconds(30),
                Duration.ofMillis(200), Duration.ofHours(1), batch, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));

        System.out.printf("timer bench: count=%d sleep=%dms tick=%dms batch=%d adaptive=%s%n",
                count, sleepMillis, tickMillis, batch, adaptive);

        try (WiggleServer server = new WiggleServer(config).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            client.register(bp);
            for (int i = 0; i < count; i++) client.start(bp, Map.of("i", (long) i));

            List<Worker> pool = new ArrayList<>();
            long t0 = System.nanoTime();
            for (int i = 0; i < workers; i++) {
                pool.add(new Worker(client, "timer-worker-" + i,
                        WorkerOptions.defaults().withConcurrency(concurrency))
                        .register(bp).handlers(new TimerHandlers(done)).start());
            }
            done.await();
            long t1 = System.nanoTime();
            for (Worker w : pool) w.close();

            double seconds = (t1 - t0) / 1e9;
            System.out.printf(
                    "timer bench: %d instances (1 timer each) in %.2fs -> %.0f timers/sec end-to-end "
                            + "(fixed-sweep floor at this cadence: %.1fs)%n",
                    count, seconds, count / seconds, (double) count / batch * (tickMillis / 1000.0));
        }
    }

    @Handlers("bench-timer")
    public static final class TimerHandlers {
        private final CountDownLatch done;
        public TimerHandlers(CountDownLatch done) { this.done = done; }
        public Map<String, Object> enter(Map<String, Object> ctx) { return ctx; }
        public void exit(Map<String, Object> ctx) { done.countDown(); }
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? def : v;
    }

    private static int intEnv(String key, int def) {
        return Integer.parseInt(env(key, String.valueOf(def)));
    }

    private TimerBench() {}
}
