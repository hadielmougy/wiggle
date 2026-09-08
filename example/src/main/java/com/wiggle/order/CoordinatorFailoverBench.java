package com.wiggle.order;

import com.wiggle.client.CoordinatedConnection;
import com.wiggle.client.WiggleClient;
import com.wiggle.client.WiggleConnection;
import com.wiggle.client.dsl.Blueprint;
import com.wiggle.core.InstanceView;
import com.wiggle.core.Tls;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Coordinator resiliency under load: submit at a fixed paced rate for the whole run and record every
 * start attempt (ok/error, per second) plus low-frequency probe sojourns — while the operator kills
 * the coordinator mid-run. Unlike {@link RateCeilingBench} the submitters survive errors (an outage
 * is the object of study, not a reason to stop), so the output shows the outage window, what failed
 * inside it, and how fast starts and sojourns recover after the coordinator comes back.
 *
 * <pre>
 *   WIGGLE_COORDINATOR_URL=127.0.0.1:18099 WIGGLE_NAMESPACE=abc \
 *   WIGGLE_ENDPOINT_REWRITE="10.244.0.15:8080=127.0.0.1:18100,10.244.0.16:8080=127.0.0.1:18101" \
 *     ./gradlew :example:coordFailover     # then kill the coordinator mid-run
 * </pre>
 *
 * Tune with {@code BENCH_RATE} (starts/sec, default 150), {@code BENCH_SECONDS} (default 240),
 * {@code BENCH_THREADS} (default 16). A worker (e.g. {@link NamespaceWorkerMain}) must be running.
 */
public final class CoordinatorFailoverBench {

    private static final long PROBE_EVERY_MILLIS = 2000;
    private static final long PROBE_TIMEOUT_MILLIS = 45_000;
    private static final long PROBE_POLL_MILLIS = 250;
    private static final long DRAIN_TIMEOUT_MILLIS = 300_000;

    record Attempt(long atMillis, boolean ok) {}
    record Probe(long atMillis, long sojournMillis) {}   // -1 = failed/timed out

    public static void main(String[] args) throws Exception {
        String coord = env("WIGGLE_COORDINATOR_URL", "127.0.0.1:18099");
        String ns = env("WIGGLE_NAMESPACE", "abc");
        int rate = Integer.parseInt(env("BENCH_RATE", "150"));
        long runMillis = Long.parseLong(env("BENCH_SECONDS", "240")) * 1000;
        int threads = Integer.parseInt(env("BENCH_THREADS", "16"));

        try (var resolver = WiggleConnection.coordinator(coord, Tls.Options.DISABLED, "us")) {
            Blueprint bp = OrderFulfilment.blueprint();
            resolver.registerWorkflow(ns, bp);

            System.out.printf("coordinator-failover bench: coordinator=%s ns=%s rate=%d/s run=%ds threads=%d%n",
                    coord, ns, rate, runMillis / 1000, threads);
            System.out.println("(kill the coordinator mid-run; submitters keep going through the outage)\n");

            ConcurrentLinkedQueue<Attempt> attempts = new ConcurrentLinkedQueue<>();
            ConcurrentLinkedQueue<Probe> probes = new ConcurrentLinkedQueue<>();
            AtomicLong slots = new AtomicLong();
            AtomicLong seq = new AtomicLong();
            long intervalNanos = 1_000_000_000L / rate;
            long t0n = System.nanoTime();
            long t0m = System.currentTimeMillis();
            long endNanos = t0n + runMillis * 1_000_000L;

            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch done = new CountDownLatch(threads);
            for (int t = 0; t < threads; t++) {
                pool.execute(() -> {
                    try {
                        while (true) {
                            long slot = slots.getAndIncrement();
                            long at = t0n + slot * intervalNanos;
                            if (at >= endNanos) return;
                            long wait = at - System.nanoTime();
                            if (wait > 0) TimeUnit.NANOSECONDS.sleep(wait);
                            long n = seq.getAndIncrement();
                            long stamp = System.currentTimeMillis() - t0m;
                            try {
                                Order order = Order.of("F-" + n, "failover-" + n, 1 + (int) (n % 3),
                                        new BigDecimal("100.00"));
                                resolver.clientForNamespace(ns).start(bp, order);   // resolve per start
                                attempts.add(new Attempt(stamp, true));
                            } catch (Exception e) {                                  // survive the outage
                                attempts.add(new Attempt(stamp, false));
                            }
                        }
                    } catch (InterruptedException ignored) {
                    } finally {
                        done.countDown();
                    }
                });
            }

            ExecutorService probePool = Executors.newCachedThreadPool();
            Thread prober = new Thread(() -> {
                try {
                    while (System.nanoTime() < endNanos) {
                        long stamp = System.currentTimeMillis() - t0m;
                        probePool.submit(() -> probes.add(new Probe(stamp, probeSojourn(resolver, ns, bp))));
                        Thread.sleep(PROBE_EVERY_MILLIS);
                    }
                } catch (InterruptedException ignored) { }
            });
            prober.start();

            done.await();
            prober.join();
            pool.shutdown();
            probePool.shutdown();
            probePool.awaitTermination(PROBE_TIMEOUT_MILLIS + 5000, TimeUnit.MILLISECONDS);

            report(attempts, probes, runMillis, rate);
            drain(resolver, ns, bp);
        }
    }

    // ---- reporting -------------------------------------------------------------------------------

    private static void report(ConcurrentLinkedQueue<Attempt> attempts, ConcurrentLinkedQueue<Probe> probes,
                               long runMillis, int rate) {
        Map<Long, long[]> buckets = new TreeMap<>();   // 5s bucket -> [ok, err]
        for (Attempt a : attempts) {
            long[] b = buckets.computeIfAbsent(a.atMillis() / 5000, k -> new long[2]);
            if (a.ok()) b[0]++; else b[1]++;
        }
        Map<Long, List<Long>> probeBuckets = new TreeMap<>();
        for (Probe p : probes) {
            probeBuckets.computeIfAbsent(p.atMillis() / 5000, k -> new ArrayList<>()).add(p.sojournMillis());
        }
        System.out.println("\n  t(s)   ok/5s  err/5s   probe sojourns (ms)");
        for (var e : buckets.entrySet()) {
            List<Long> pb = probeBuckets.getOrDefault(e.getKey(), List.of());
            String probeStr = pb.isEmpty() ? "" : pb.stream().sorted()
                    .map(v -> v < 0 ? "FAIL" : String.valueOf(v))
                    .reduce((a, b) -> a + " " + b).orElse("");
            System.out.printf("  %4d   %5d  %6s   %s%n", e.getKey() * 5, e.getValue()[0],
                    e.getValue()[1] == 0 ? "-" : String.valueOf(e.getValue()[1]), probeStr);
        }

        long ok = attempts.stream().filter(Attempt::ok).count();
        long err = attempts.size() - ok;
        long firstErr = attempts.stream().filter(a -> !a.ok()).mapToLong(Attempt::atMillis).min().orElse(-1);
        long lastErr = attempts.stream().filter(a -> !a.ok()).mapToLong(Attempt::atMillis).max().orElse(-1);
        // Largest gap between consecutive successful starts = the true start-availability outage.
        long maxGap = 0;
        long prev = -1;
        for (Attempt a : attempts.stream().filter(Attempt::ok)
                .sorted(Comparator.comparingLong(Attempt::atMillis)).toList()) {
            if (prev >= 0) maxGap = Math.max(maxGap, a.atMillis() - prev);
            prev = a.atMillis();
        }
        List<Long> tail = probes.stream()
                .filter(p -> p.atMillis() > runMillis - 30_000 && p.sojournMillis() >= 0)
                .map(Probe::sojournMillis).sorted().toList();
        long tailMed = tail.isEmpty() ? -1 : tail.get(tail.size() / 2);

        System.out.printf("%nsummary: attempts=%d ok=%d err=%d (%.2f%%)  target=%d/s achieved(ok)=%.0f/s%n",
                attempts.size(), ok, err, 100.0 * err / Math.max(1, attempts.size()),
                rate, ok / (runMillis / 1000.0));
        if (err > 0) {
            System.out.printf("error window: t=%.1fs .. t=%.1fs (%.1fs)  max successful-start gap=%.1fs%n",
                    firstErr / 1000.0, lastErr / 1000.0, (lastErr - firstErr) / 1000.0, maxGap / 1000.0);
        } else {
            System.out.println("no start errors at all (outage fully absorbed by retries/timing)");
        }
        long probeFails = probes.stream().filter(p -> p.sojournMillis() < 0).count();
        System.out.printf("probes: %d total, %d failed;  last-30s sojourn median=%dms%n",
                probes.size(), probeFails, tailMed);
    }

    /** Start one probe instance and poll it to a terminal state; -1 on any failure or timeout. */
    private static long probeSojourn(CoordinatedConnection resolver, String ns, Blueprint bp) {
        long s = System.nanoTime();
        try {
            Order order = Order.of("FP-" + s, "probe", 1, new BigDecimal("1.00"));
            String id = resolver.clientForNamespace(ns).start(bp, order);
            long deadline = s + PROBE_TIMEOUT_MILLIS * 1_000_000L;
            while (System.nanoTime() < deadline) {
                InstanceView v = resolver.clientForInstance(id).instance(id);
                if (v.isTerminal()) return (System.nanoTime() - s) / 1_000_000;
                Thread.sleep(PROBE_POLL_MILLIS);
            }
        } catch (Exception ignored) { }
        return -1;
    }

    private static final Map<String, WiggleClient> CELLS = new ConcurrentHashMap<>();

    /** Post-run integrity: wait until RUNNING across every active cell stays ~0 — nothing stuck. */
    private static void drain(CoordinatedConnection resolver, String ns, Blueprint bp) throws Exception {
        long s = System.currentTimeMillis();
        int consecutive = 0;
        String perCell = "";
        while (System.currentTimeMillis() - s < DRAIN_TIMEOUT_MILLIS) {
            int total = 0;
            StringBuilder counts = new StringBuilder();
            for (String target : resolver.activeCellTargets(ns)) {
                WiggleClient c = CELLS.computeIfAbsent(target, t -> new WiggleClient(t, Tls.Options.DISABLED));
                int n = c.listInstances(bp.name(), "RUNNING", 2000).size();
                total += n;
                if (counts.length() > 0) counts.append(", ");
                counts.append(target).append('=').append(n);
            }
            perCell = counts.toString();
            consecutive = total < 50 ? consecutive + 1 : 0;
            if (consecutive >= 2) {
                System.out.printf("drained in %.1fs — running: %s%n", (System.currentTimeMillis() - s) / 1000.0, perCell);
                return;
            }
            Thread.sleep(2000);
        }
        System.out.println("DRAIN TIMEOUT — still RUNNING: " + perCell);
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? def : v;
    }

    private CoordinatorFailoverBench() {}
}
