package com.wiggle.order;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.Activity;
import com.wiggle.client.worker.Compensable;
import com.wiggle.client.worker.CompensableActivity;
import com.wiggle.client.worker.Compensation;
import com.wiggle.client.worker.ForFlow;
import com.wiggle.client.worker.PermanentActivityException;
import com.wiggle.client.worker.Worker;
import com.wiggle.client.worker.WorkerOptions;
import com.wiggle.core.InstanceView;
import com.wiggle.core.Tls;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Load-tests the saga reverse pass: every instance runs two compensable steps and then fails
 * permanently, so the engine must mint and drain two compensation tasks per instance on top of the
 * three forward tasks. Reports the achieved start rate, time to full drain (all instances
 * COMPENSATED), effective task throughput (forward + reverse), and a status histogram — any
 * outcome other than COMPENSATED is a correctness failure, printed loudly.
 *
 * <p>Runs against the server at {@code WIGGLE_SERVER_URL} (default {@code 127.0.0.1:8080}), like
 * {@link RateCeilingBench}, and brings its own worker. Tune with {@code BENCH_COUNT} (2000), {@code BENCH_RATE} (starts/sec,
 * 200), {@code BENCH_THREADS} (8).
 */
public final class SagaLoadBench {

    interface SagaSteps {
        CompensableActivity<Map<String, Object>, Map<String, Object>> reserve();
        CompensableActivity<Map<String, Object>, Map<String, Object>> enrich();
        Map<String, Object> boom(Map<String, Object> ctx);
    }

    static final AtomicLong UNDOS = new AtomicLong();

    /** reserve(compensable) -> enrich (replaces the context) -> boom (permanent failure). */
    static FlowSpec flowSpec() {
        return FlowSpec.define("saga-load", 1, Map.class, SagaSteps.class, (f, s) -> f
                .thenApplyCompensable(s::reserve)
                .thenApplyCompensable(s::enrich)
                .thenApply(s::boom));
    }

    @ForFlow("saga-load")
    public static final class SagaHandlers {
        public CompensableActivity<Map<String, Object>, Map<String, Object>> reserve() { return compensable("reservationRef"); }
        public CompensableActivity<Map<String, Object>, Map<String, Object>> enrich() { return compensable("enrichmentRef"); }
        public Map<String, Object> boom(Map<String, Object> ctx) {
            throw new PermanentActivityException("saga-load: forced failure");
        }

        private static CompensableActivity<Map<String, Object>, Map<String, Object>> compensable(String key) {
            final class Step implements CompensableActivity<Map<String, Object>, Map<String, Object>> {
                public Map<String, Object> execute(Map<String, Object> ctx) {
                    Map<String, Object> next = new LinkedHashMap<>(ctx);
                    next.put(key, "ref-" + ctx.get("seq"));
                    return next;
                }
                public void compensate(Compensation<Map<String, Object>, Map<String, Object>> c) {
                    // the snapshot contract, checked under load: this step's product must be present
                    if (c.result().get(key) == null) {
                        throw new IllegalStateException("undo of " + key + " got a snapshot without it");
                    }
                    UNDOS.incrementAndGet();
                }
            }
            return new Step();
        }
    }

    public static void main(String[] args) throws Exception {
        String server = env("WIGGLE_SERVER_URL", "127.0.0.1:8080");
        int count = Integer.parseInt(env("BENCH_COUNT", "2000"));
        int rate = Integer.parseInt(env("BENCH_RATE", "200"));
        int threads = Integer.parseInt(env("BENCH_THREADS", "8"));

        try (WiggleClient client = new WiggleClient(server, Tls.Options.DISABLED)) {
            FlowSpec bp = flowSpec();
            client.register(bp);

            Worker worker = new Worker(client, "saga-load",
                    WorkerOptions.defaults().withConcurrency(100).withLongPollWait(Duration.ofSeconds(10)))
                    .registerHandler(new SagaHandlers());
            worker.start();

            System.out.printf("saga load: %d instances at %d/s (%d threads) via %s%n",
                    count, rate, threads, server);

            ConcurrentLinkedQueue<String> ids = new ConcurrentLinkedQueue<>();
            AtomicLong seq = new AtomicLong();
            long intervalNanos = 1_000_000_000L / rate;
            long t0 = System.nanoTime();
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch submitted = new CountDownLatch(threads);
            for (int t = 0; t < threads; t++) {
                pool.execute(() -> {
                    try {
                        while (true) {
                            long slot = seq.getAndIncrement();
                            if (slot >= count) return;
                            long at = t0 + slot * intervalNanos;
                            long wait = at - System.nanoTime();
                            if (wait > 0) TimeUnit.NANOSECONDS.sleep(wait);
                            ids.add(client.start(bp, Map.of("seq", slot)));
                        }
                    } catch (Exception e) {
                        System.err.println("submitter died: " + e);
                    } finally {
                        submitted.countDown();
                    }
                });
            }
            submitted.await();
            pool.shutdown();
            double submitSecs = (System.nanoTime() - t0) / 1e9;
            System.out.printf("submitted %d in %.1fs (%.0f/s)%n", ids.size(), submitSecs, ids.size() / submitSecs);

            // Poll every instance to a terminal state; histogram the outcomes.
            ExecutorService pollPool = Executors.newFixedThreadPool(32);
            List<java.util.concurrent.Future<String>> outcomes = new ArrayList<>();
            long pollDeadline = System.nanoTime() + Duration.ofMinutes(10).toNanos();
            for (String id : ids) {
                outcomes.add(pollPool.submit(() -> {
                    while (System.nanoTime() < pollDeadline) {
                        InstanceView v = client.instance(id);
                        if (v.isTerminal()) return v.status();
                        Thread.sleep(200);
                    }
                    return "TIMED_OUT";
                }));
            }
            Map<String, Integer> histogram = new java.util.TreeMap<>();
            for (var f : outcomes) histogram.merge(f.get(), 1, Integer::sum);
            double totalSecs = (System.nanoTime() - t0) / 1e9;
            pollPool.shutdownNow();
            worker.close();

            long tasks = (long) ids.size() * 5;   // 3 forward + 2 compensations per instance
            System.out.printf("%ndrained in %.1fs total (%.0f instances/s end-to-end, ~%.0f tasks/s incl. reverse pass)%n",
                    totalSecs, ids.size() / totalSecs, tasks / totalSecs);
            System.out.println("outcomes: " + histogram);
            System.out.printf("compensators executed: %d (expected >= %d; at-least-once may exceed)%n",
                    UNDOS.get(), (long) ids.size() * 2);
            boolean allCompensated = histogram.getOrDefault("COMPENSATED", 0) == ids.size();
            System.out.println(allCompensated
                    ? "== PASS: every instance COMPENSATED under load =="
                    : "== FAIL: outcomes other than COMPENSATED present ==");
            if (!allCompensated) System.exit(1);
        }
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? def : v;
    }
}
