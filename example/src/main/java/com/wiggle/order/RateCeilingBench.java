package com.wiggle.order;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.core.InstanceView;
import com.wiggle.core.Tls;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
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
 *   WIGGLE_SERVER_URL=127.0.0.1:8080 ./gradlew :example:rateCeiling
 * </pre>
 *
 * Tune with {@code BENCH_RATES} (csv, default 300..1000), {@code BENCH_STAGE_SECONDS} (default 30),
 * {@code BENCH_CONFIRM_SECONDS} (default 60), {@code BENCH_THREADS} (default 16). A worker (e.g.
 * {@link WorkerMain}) must be running against the same server.
 */
public final class RateCeilingBench {

    private static final long PROBE_EVERY_MILLIS = 2000;
    private static final long PROBE_TIMEOUT_MILLIS = 45_000;
    private static final long PROBE_POLL_MILLIS = 250;
    private static final long DRAIN_TIMEOUT_MILLIS = 300_000;

    record ProbeSample(long atStageMillis, long sojournMillis) {}   // sojourn -1 = timed out

    record StageResult(int targetRate, double achievedRate, long submitP50, long submitP99, long maxLagMillis,
                       long probeFirstMed, long probeLastMed, int probes, boolean pass, String note) {}

    public static void main(String[] args) throws Exception {
        String server = env("WIGGLE_SERVER_URL", "127.0.0.1:8080");
        int[] rates = parseRates(env("BENCH_RATES", "300,400,500,600,700,800,900,1000"));
        long stageMillis = Long.parseLong(env("BENCH_STAGE_SECONDS", "30")) * 1000;
        long confirmMillis = Long.parseLong(env("BENCH_CONFIRM_SECONDS", "60")) * 1000;
        int threads = Integer.parseInt(env("BENCH_THREADS", "16"));

        try (WiggleClient target = new WiggleClient(server, Tls.Options.DISABLED)) {
            FlowSpec bp = OrderFulfilment.flowSpec();
            target.register(bp);

            System.out.printf("rate-ceiling bench: server=%s stage=%ds threads=%d rates=%s%n",
                    server, stageMillis / 1000, threads, java.util.Arrays.toString(rates));
            System.out.println("(a worker must be running; verdicts come from probe sojourn drift)\n");

            drain(target, bp, "warm-up");

            List<StageResult> results = new ArrayList<>();
            int ceiling = -1;
            for (int rate : rates) {
                StageResult r = stage(target, bp, rate, stageMillis, threads);
                results.add(r);
                System.out.println(format(r));
                long drained = drain(target, bp, "after " + rate + "/s");
                if (!r.pass()) {
                    System.out.printf("   backlog at failure took %ds to drain%n", drained / 1000);
                    break;
                }
                ceiling = rate;
            }

            if (ceiling > 0 && confirmMillis > 0) {
                System.out.printf("%nconfirming %d/s over %ds…%n", ceiling, confirmMillis / 1000);
                StageResult confirm = stage(target, bp, ceiling, confirmMillis, threads);
                System.out.println(format(confirm));
                drain(target, bp, "after confirm");
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

    /** Run one paced stage at {@code rate}/s and judge it by probe-sojourn drift. */
    private static StageResult stage(WiggleClient target, FlowSpec bp, int rate,
                                     long stageMillis, int threads) throws Exception {
        ConcurrentLinkedQueue<Long> submitNanos = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<ProbeSample> probes = new ConcurrentLinkedQueue<>();
        AtomicLong slots = new AtomicLong();
        AtomicLong started = new AtomicLong();
        AtomicLong maxLagNanos = new AtomicLong();
        AtomicBoolean failed = new AtomicBoolean();
        long intervalNanos = 1_000_000_000L / rate;
        long t0 = System.nanoTime();
        long endNanos = t0 + stageMillis * 1_000_000L;

        // Paced submitters: thread-shared slot counter → evenly spaced arrivals at the target rate. A slot
        // the submitters only reach after the stage has ended is dropped, not issued late, so the starts
        // counted are the ones the server received inside the stage and the achieved rate is real.
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
                        else maxLagNanos.accumulateAndGet(-wait, Math::max);
                        if (System.nanoTime() >= endNanos) return;
                        long seq = started.getAndIncrement();
                        Order order = Order.of("B-" + seq, "bench-" + seq, 1 + (int) (seq % 3),
                                new BigDecimal("100.00"));
                        long s = System.nanoTime();
                        target.start(bp, order);
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
                            probes.add(new ProbeSample(atStage, probeSojourn(target, bp)))));
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
                : submitterBound ? "submitter-bound: starts fell behind the target rate (slow submits = the "
                        + "server's start path; fast submits = add BENCH_THREADS)"
                : timedOut ? "probe timed out (> " + PROBE_TIMEOUT_MILLIS / 1000 + "s)"
                : piling ? "sojourn drifting up — queue piling" : "stable";
        return new StageResult(rate, achieved, p50, p99, maxLagNanos.get() / 1_000_000,
                midMed, lastMed, ordered.size(), pass, note);
    }

    /** Start one probe instance and poll it to a terminal state; -1 on timeout. */
    private static long probeSojourn(WiggleClient target, FlowSpec bp) {
        long s = System.nanoTime();
        try {
            Order order = Order.of("PROBE-" + s, "probe", 1, new BigDecimal("1.00"));
            String id = target.start(bp, order);
            long deadline = s + PROBE_TIMEOUT_MILLIS * 1_000_000L;
            while (System.nanoTime() < deadline) {
                InstanceView v = target.instance(id);
                if (v.isTerminal()) return (System.nanoTime() - s) / 1_000_000;
                Thread.sleep(PROBE_POLL_MILLIS);
            }
        } catch (Exception e) {
            System.err.println("probe failed: " + e);
        }
        return -1;
    }

    /**
     * Wait until the backlog is actually gone: the RUNNING count for the workflow stays small over two
     * consecutive checks. (A fresh probe completing fast is NOT proof — dispatch isn't FIFO per
     * instance, so a new instance's first step can jump ahead of older instances' queued branches.)
     */
    private static long drain(WiggleClient target, FlowSpec bp, String label)
            throws Exception {
        long s = System.currentTimeMillis();
        int consecutive = 0;
        int running = 0;
        while (System.currentTimeMillis() - s < DRAIN_TIMEOUT_MILLIS) {
            running = target.listInstances(bp.name(), "RUNNING", 2000).size();
            consecutive = running < 50 ? consecutive + 1 : 0;
            if (consecutive >= 2) {
                long took = System.currentTimeMillis() - s;
                System.out.printf("   drained (%s) in %.1fs — running: %d%n", label, took / 1000.0, running);
                return took;
            }
            Thread.sleep(2000);
        }
        System.out.println("   DRAIN TIMEOUT (" + label + ") — still RUNNING: " + running);
        return DRAIN_TIMEOUT_MILLIS;
    }

    private static long median(List<ProbeSample> xs) {
        long[] v = xs.stream().mapToLong(ProbeSample::sojournMillis).filter(x -> x >= 0).sorted().toArray();
        return v.length == 0 ? -1 : v[v.length / 2];
    }

    private static String format(StageResult r) {
        return String.format("rate=%d/s  achieved=%.0f/s  submit p50=%dms p99=%dms  max lag=%dms  " +
                        "probe sojourn first½=%dms last½=%dms (%d probes)  %s  [%s]",
                r.targetRate(), r.achievedRate(), r.submitP50(), r.submitP99(), r.maxLagMillis(),
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
