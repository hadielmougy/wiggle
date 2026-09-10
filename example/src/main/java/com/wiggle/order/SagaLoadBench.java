package com.wiggle.order;

import com.wiggle.client.CoordinatedConnection;
import com.wiggle.client.WiggleClient;
import com.wiggle.client.WiggleConnection;
import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.client.worker.Activity;
import com.wiggle.client.worker.Compensable;
import com.wiggle.client.worker.Compensation;
import com.wiggle.client.worker.Handlers;
import com.wiggle.client.worker.NamespaceWorker;
import com.wiggle.client.worker.PermanentActivityException;
import com.wiggle.client.worker.WorkerOptions;
import com.wiggle.core.InstanceView;
import com.wiggle.core.Tls;

import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
 * <p>Runs against a coordinator deployment like {@link RateCeilingBench} (same env:
 * {@code WIGGLE_COORDINATOR_URL}, {@code WIGGLE_NAMESPACE}, {@code WIGGLE_ENDPOINT_REWRITE}), and
 * brings its own worker. Tune with {@code BENCH_COUNT} (2000), {@code BENCH_RATE} (starts/sec,
 * 200), {@code BENCH_THREADS} (8).
 */
public final class SagaLoadBench {

    static final AtomicLong UNDOS = new AtomicLong();

    /** reserve(compensable) -> enrich (replaces the context) -> boom (permanent failure). */
    static Blueprint blueprint() {
        return Workflow.define("saga-load")
                .step("reserve").compensate()
                .step("enrich").compensate()
                .step("boom")
                .build();
    }

    @Handlers("saga-load")
    public static final class SagaHandlers {
        public Activity<Map<String, Object>> reserve() { return compensable("reservationRef"); }
        public Activity<Map<String, Object>> enrich() { return compensable("enrichmentRef"); }
        public Map<String, Object> boom(Map<String, Object> ctx) {
            throw new PermanentActivityException("saga-load: forced failure");
        }

        private static Activity<Map<String, Object>> compensable(String key) {
            final class Step implements Activity<Map<String, Object>>, Compensable<Map<String, Object>> {
                public Map<String, Object> execute(Map<String, Object> ctx) {
                    Map<String, Object> next = new LinkedHashMap<>(ctx);
                    next.put(key, "ref-" + ctx.get("seq"));
                    return next;
                }
                public void compensate(Compensation<Map<String, Object>> c) {
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
        String coord = env("WIGGLE_COORDINATOR_URL", "127.0.0.1:18099");
        String ns = env("WIGGLE_NAMESPACE", "abc");
        int count = Integer.parseInt(env("BENCH_COUNT", "2000"));
        int rate = Integer.parseInt(env("BENCH_RATE", "200"));
        int threads = Integer.parseInt(env("BENCH_THREADS", "8"));

        try (CoordinatedConnection resolver = WiggleConnection.coordinator(coord, Tls.Options.DISABLED, "saga")) {
            Blueprint bp = blueprint();
            resolver.registerWorkflow(ns, bp);

            NamespaceWorker worker = new NamespaceWorker(
                    () -> resolver.activeCellTargets(ns),
                    WiggleClient::new,
                    "saga-load",
                    WorkerOptions.defaults().withConcurrency(100).withLongPollWait(Duration.ofSeconds(10)),
                    w -> w.register(bp).handlers(new SagaHandlers())
            ).start();

            System.out.printf("saga load: %d instances at %d/s (%d threads) via %s ns=%s%n",
                    count, rate, threads, coord, ns);

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
                            ids.add(resolver.clientForNamespace(ns).start(bp, Map.of("seq", slot)));
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
            Map<String, WiggleClient> cellCache = new ConcurrentHashMap<>();
            ExecutorService pollPool = Executors.newFixedThreadPool(32);
            List<java.util.concurrent.Future<String>> outcomes = new ArrayList<>();
            long pollDeadline = System.nanoTime() + Duration.ofMinutes(10).toNanos();
            for (String id : ids) {
                outcomes.add(pollPool.submit(() -> {
                    while (System.nanoTime() < pollDeadline) {
                        InstanceView v = resolver.clientForInstance(id).instance(id);
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
