package com.wiggle.order;

import com.wiggle.client.CoordinatedConnection;
import com.wiggle.client.WiggleClient;
import com.wiggle.client.WiggleConnection;
import com.wiggle.client.dsl.Blueprint;
import com.wiggle.core.InstanceView;
import com.wiggle.core.Tls;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Finds the sustainable start-rate ceiling of a deployment: the highest starts/sec at which the
 * server keeps up, and the first rate at which the queue starts to pile up.
 *
 * <p>It ramps through a ladder of target rates (paced, evenly spaced arrivals from several
 * submitter threads). While each stage runs, a low-frequency <b>probe</b> starts one instance every
 * couple of seconds and measures its <em>sojourn</em> — submit to COMPLETED. Under capacity the
 * sojourn is flat (roughly the service time); above capacity every new arrival waits behind a
 * growing backlog, so probe sojourn climbs monotonically through the stage. That drift, not a
 * server-side metric, is the "no longer catching up" signal — it needs no heavy list queries that
 * would distort the measurement.
 *
 * <p>Between stages it drains (probes until sojourn is small again) so stages don't contaminate
 * each other, and it re-confirms the found ceiling with one longer stage.
 *
 * <pre>
 *   WIGGLE_COORDINATOR_URL=127.0.0.1:18099 WIGGLE_NAMESPACE=abc \
 *   WIGGLE_ENDPOINT_REWRITE="10.244.0.7:8080=127.0.0.1:18100" \
 *     ./gradlew :example:rateCeiling
 * </pre>
 *
 * Tune with {@code BENCH_RATES} (csv, default 300..1000), {@code BENCH_STAGE_SECONDS} (default 30),
 * {@code BENCH_CONFIRM_SECONDS} (default 60), {@code BENCH_THREADS} (default 16). A worker (e.g.
 * {@link NamespaceWorkerMain}) must be running against the same deployment.
 */
public final class RateCeilingBench {

    private static final long PROBE_EVERY_MILLIS = 2000;
    private static final long PROBE_TIMEOUT_MILLIS = 45_000;
    private static final long PROBE_POLL_MILLIS = 250;
    private static final long DRAIN_TIMEOUT_MILLIS = 300_000;

    record ProbeSample(long atStageMillis, long sojournMillis) {}   // sojourn -1 = timed out

    record StageResult(int targetRate, double achievedRate, long submitP50, long submitP99,
                       long probeFirstMed, long probeLastMed, int probes, boolean pass, String note) {}

    public static void main(String[] args) throws Exception {
        String coord = env("WIGGLE_COORDINATOR_URL", "127.0.0.1:18099");
        String ns = env("WIGGLE_NAMESPACE", "abc");
        int[] rates = parseRates(env("BENCH_RATES", "300,400,500,600,700,800,900,1000"));
        long stageMillis = Long.parseLong(env("BENCH_STAGE_SECONDS", "30")) * 1000;
        long confirmMillis = Long.parseLong(env("BENCH_CONFIRM_SECONDS", "60")) * 1000;
        int threads = Integer.parseInt(env("BENCH_THREADS", "16"));

        try (var resolver = WiggleConnection.coordinator(coord, Tls.Options.DISABLED, "us")) {
            Blueprint bp = OrderFulfilment.blueprint();
            resolver.registerWorkflow(ns, bp);

            System.out.printf("rate-ceiling bench: coordinator=%s namespace=%s stage=%ds threads=%d rates=%s%n",
                    coord, ns, stageMillis / 1000, threads, java.util.Arrays.toString(rates));
            System.out.println("(a worker must be running; verdicts come from probe sojourn drift)\n");

            drain(resolver, ns, bp, "warm-up");

            List<StageResult> results = new ArrayList<>();
            int ceiling = -1;
            for (int rate : rates) {
                StageResult r = stage(resolver, ns, bp, rate, stageMillis, threads);
                results.add(r);
                System.out.println(format(r));
                long drained = drain(resolver, ns, bp, "after " + rate + "/s");
                if (!r.pass()) {
                    System.out.printf("   backlog at failure took %ds to drain%n", drained / 1000);
                    break;
                }
                ceiling = rate;
            }

            if (ceiling > 0 && confirmMillis > 0) {
                System.out.printf("%nconfirming %d/s over %ds…%n", ceiling, confirmMillis / 1000);
                StageResult confirm = stage(resolver, ns, bp, ceiling, confirmMillis, threads);
                System.out.println(format(confirm));
                drain(resolver, ns, bp, "after confirm");
                if (!confirm.pass()) {
                    System.out.printf("%n== ceiling: UNSTABLE at %d/s over %ds — the sustainable rate is just below it ==%n",
                            ceiling, confirmMillis / 1000);
                } else {
                    System.out.printf("%n== ceiling: %d/s sustained for %ds with flat sojourn ==%n",
                            ceiling, confirmMillis / 1000);
                }
            } else if (ceiling < 0) {
                System.out.println("\n== even the first rate exceeded capacity — lower BENCH_RATES ==");
            }

            System.out.println("\nsummary:");
            for (StageResult r : results) System.out.println("  " + format(r));
        }
    }

    /**
     * Run one paced stage at {@code rate}/s and judge it by probe-sojourn drift. Each start resolves
     * the namespace afresh ({@code clientForNamespace} per call) — that per-start resolve is what
     * spreads new instances across a multi-cell ring; a cached client would pin them to one cell.
     */
    private static StageResult stage(CoordinatedConnection resolver, String ns, Blueprint bp, int rate,
                                     long stageMillis, int threads) throws Exception {
        ConcurrentLinkedQueue<Long> submitNanos = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<ProbeSample> probes = new ConcurrentLinkedQueue<>();
        AtomicLong slots = new AtomicLong();
        AtomicLong started = new AtomicLong();
        AtomicBoolean failed = new AtomicBoolean();
        long intervalNanos = 1_000_000_000L / rate;
        long t0 = System.nanoTime();
        long endNanos = t0 + stageMillis * 1_000_000L;

        // Paced submitters: thread-shared slot counter → evenly spaced arrivals at the target rate.
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch done = new CountDownLatch(threads);
        for (int t = 0; t < threads; t++) {
            pool.execute(() -> {
                try {
                    while (true) {
                        long slot = slots.getAndIncrement();
                        long at = t0 + slot * intervalNanos;
                        if (at >= endNanos) return;
                        long wait = at - System.nanoTime();
                        if (wait > 0) TimeUnit.NANOSECONDS.sleep(wait);
                        long seq = started.getAndIncrement();
                        Order order = Order.of("B-" + seq, "bench-" + seq, 1 + (int) (seq % 3),
                                new BigDecimal("100.00"));
                        long s = System.nanoTime();
                        resolver.clientForNamespace(ns).start(bp, order);   // resolve per start -> spread
                        submitNanos.add(System.nanoTime() - s);
                    }
                } catch (Exception e) {
                    failed.set(true);
                    System.err.println("submitter died: " + e);
                } finally {
                    done.countDown();
                }
            });
        }

        // Probes: one instance every PROBE_EVERY_MILLIS, sojourn measured by polling to terminal.
        ExecutorService probePool = Executors.newCachedThreadPool();
        List<Future<?>> probeFutures = new ArrayList<>();
        Thread prober = new Thread(() -> {
            try {
                while (System.nanoTime() < endNanos) {
                    long atStage = (System.nanoTime() - t0) / 1_000_000;
                    probeFutures.add(probePool.submit(() ->
                            probes.add(new ProbeSample(atStage, probeSojourn(resolver, ns, bp)))));
                    Thread.sleep(PROBE_EVERY_MILLIS);
                }
            } catch (InterruptedException ignored) { }
        });
        prober.start();

        done.await();
        prober.join();
        pool.shutdown();
        for (Future<?> f : probeFutures) {
            try { f.get(PROBE_TIMEOUT_MILLIS + 5000, TimeUnit.MILLISECONDS); } catch (Exception ignored) { }
        }
        probePool.shutdownNow();

        double achieved = started.get() / (stageMillis / 1000.0);
        long[] lat = submitNanos.stream().mapToLong(Long::longValue).sorted().toArray();
        long p50 = lat.length == 0 ? 0 : lat[lat.length / 2] / 1_000_000;
        long p99 = lat.length == 0 ? 0 : lat[(int) (lat.length * 0.99)] / 1_000_000;

        List<ProbeSample> ordered = probes.stream()
                .sorted(java.util.Comparator.comparingLong(ProbeSample::atStageMillis)).toList();
        for (ProbeSample pr : ordered) {   // the raw curve, for offline inspection (ramp vs unbounded growth)
            System.out.printf("      probe t=%ds sojourn=%dms%n", pr.atStageMillis() / 1000, pr.sojournMillis());
        }
        boolean timedOut = ordered.stream().anyMatch(pr -> pr.sojournMillis() < 0);
        // Judge by the MIDDLE vs LAST third: the first third is the ramp from an empty queue to the
        // standing plateau and would count as drift; only growth past the plateau means piling.
        long midMed = median(ordered.subList(ordered.size() / 3, 2 * ordered.size() / 3));
        long lastMed = median(ordered.subList(2 * ordered.size() / 3, ordered.size()));

        boolean submitterBound = achieved < rate * 0.97;
        boolean piling = timedOut || (lastMed - midMed) > 3000;
        boolean pass = !failed.get() && !submitterBound && !piling;
        String note = failed.get() ? "submitter error"
                : submitterBound ? "submitter-bound (client could not reach the target rate)"
                : timedOut ? "probe timed out (> " + PROBE_TIMEOUT_MILLIS / 1000 + "s)"
                : piling ? "sojourn drifting up — queue piling" : "stable";
        return new StageResult(rate, achieved, p50, p99, midMed, lastMed, ordered.size(), pass, note);
    }

    /** Start one probe instance and poll it to a terminal state; -1 on timeout. The poll routes by the
     *  instance id ({@code clientForInstance}), so it reads the cell that actually owns the probe. */
    private static long probeSojourn(CoordinatedConnection resolver, String ns, Blueprint bp) {
        long s = System.nanoTime();
        try {
            Order order = Order.of("PROBE-" + s, "probe", 1, new BigDecimal("1.00"));
            String id = resolver.clientForNamespace(ns).start(bp, order);
            long deadline = s + PROBE_TIMEOUT_MILLIS * 1_000_000L;
            while (System.nanoTime() < deadline) {
                InstanceView v = resolver.clientForInstance(id).instance(id);
                if (v.isTerminal()) return (System.nanoTime() - s) / 1_000_000;
                Thread.sleep(PROBE_POLL_MILLIS);
            }
        } catch (Exception e) {
            System.err.println("probe failed: " + e);
        }
        return -1;
    }

    private static final Map<String, WiggleClient> CELLS = new ConcurrentHashMap<>();

    /**
     * Wait until the backlog is actually gone: the RUNNING count for the workflow — summed across
     * every active cell of the namespace — stays small over two consecutive checks. (A fresh probe
     * completing fast is NOT proof — dispatch isn't FIFO per instance, so a new instance's first step
     * can jump ahead of older instances' queued branches.) The per-cell counts double as a check that
     * the epoch ring actually spreads load across the cells.
     */
    private static long drain(CoordinatedConnection resolver, String ns, Blueprint bp, String label)
            throws Exception {
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
                long took = System.currentTimeMillis() - s;
                System.out.printf("   drained (%s) in %.1fs — running: %s%n", label, took / 1000.0, perCell);
                return took;
            }
            Thread.sleep(2000);
        }
        System.out.println("   DRAIN TIMEOUT (" + label + ") — still RUNNING: " + perCell);
        return DRAIN_TIMEOUT_MILLIS;
    }

    private static long median(List<ProbeSample> xs) {
        long[] v = xs.stream().mapToLong(ProbeSample::sojournMillis).filter(x -> x >= 0).sorted().toArray();
        return v.length == 0 ? -1 : v[v.length / 2];
    }

    private static String format(StageResult r) {
        return String.format("rate=%d/s  achieved=%.0f/s  submit p50=%dms p99=%dms  " +
                        "probe sojourn first½=%dms last½=%dms (%d probes)  %s  [%s]",
                r.targetRate(), r.achievedRate(), r.submitP50(), r.submitP99(),
                r.probeFirstMed(), r.probeLastMed(), r.probes(), r.pass() ? "PASS" : "FAIL", r.note());
    }

    private static int[] parseRates(String csv) {
        return java.util.Arrays.stream(csv.split(",")).map(String::trim)
                .filter(s -> !s.isEmpty()).mapToInt(Integer::parseInt).toArray();
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? def : v;
    }

    private RateCeilingBench() {}
}
