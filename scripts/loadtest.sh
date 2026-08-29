#!/usr/bin/env bash
#
# A repeatable load harness for Wiggle. Ramps the submit rate against a target and reports, per step,
# the achieved submit/completion throughput and end-to-end latency percentiles -- so you can find a
# single node's ceiling and compare it against a cellular (coordinator) deployment.
#
# It measures END-TO-END latency: the time from the client calling start() to the flow's step running
# on a worker (recorded worker-side, no polling overhead). Workers run in-process, so a target is all
# you need. It does NOT start a cluster -- point it at one (e.g. the playground, or your own node).
#
# Usage:
#   scripts/loadtest.sh --server 127.0.0.1:8081                       # a single node (direct)
#   scripts/loadtest.sh --coordinator 127.0.0.1:8099 --namespace orders   # cellular (fans across cells)
#
# Options (all optional):
#   --rates 20,40,80,160,320   submit rates (processes/sec) to step through   [default 20,40,80,160,320]
#   --step 12                  seconds to hold each rate                       [default 12]
#   --workers 2                in-process workers                              [default 2]
#   --concurrency N            max in-flight steps per worker                  [default = CPUs]
#   --drain 20                 max seconds to let a step drain before the next [default 20]
#
# Example against the playground's two-cell namespace:
#   scripts/playground.sh up
#   scripts/loadtest.sh --coordinator 127.0.0.1:8099 --namespace orders --rates 50,100,200,400
set -uo pipefail
cd "$(dirname "$0")/.."

OUT=$(mktemp -d)
trap 'rm -rf "$OUT"' EXIT

if [ ! -d cli/build/install/wiggle/lib ]; then
    echo "== building the client classpath (:cli:installDist) ==" >&2
    ./gradlew :cli:installDist -q || { echo "build failed" >&2; exit 1; }
fi
CP=$(printf '%s:' cli/build/install/wiggle/lib/*.jar)

cat > "$OUT/LoadTest.java" <<'JAVA'
import dev.wiggle.client.CellResolver;
import dev.wiggle.client.WiggleClient;
import dev.wiggle.client.dsl.ActivityHandler;
import dev.wiggle.client.dsl.Blueprint;
import dev.wiggle.client.dsl.Workflow;
import dev.wiggle.client.worker.NamespaceWorker;
import dev.wiggle.client.worker.Worker;
import dev.wiggle.client.worker.WorkerOptions;
import dev.wiggle.core.Tls;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/** A small load driver: in-process workers + a paced submitter + worker-side latency recording. */
public class LoadTest {

    /** Lock-free-ish latency sink: append into a capped buffer, sort on snapshot for percentiles. */
    static final class Rec {
        final long[] buf; final AtomicInteger idx = new AtomicInteger();
        Rec(int cap) { buf = new long[Math.max(1024, cap)]; }
        void record(long v) { int i = idx.getAndIncrement(); if (i < buf.length) buf[i] = v; }
        int count() { return Math.min(idx.get(), buf.length); }
        long pct(double p) {
            int n = count(); if (n == 0) return -1;
            long[] a = Arrays.copyOf(buf, n); Arrays.sort(a);
            int i = (int) Math.ceil(p / 100.0 * n) - 1;
            return a[Math.max(0, Math.min(n - 1, i))];
        }
        long max() { int n = count(); long m = -1; for (int i = 0; i < n; i++) m = Math.max(m, buf[i]); return m; }
    }

    public static void main(String[] args) throws Exception {
        java.util.Locale.setDefault(java.util.Locale.ROOT);   // stable '.'-decimals in the report
        Map<String, String> o = parse(args);
        String server = o.get("server");
        String coord = o.get("coordinator");
        String ns = o.getOrDefault("namespace", "load");
        if ((server == null) == (coord == null)) {
            System.err.println("give exactly one of --server <host:port> or --coordinator <host:port>");
            System.exit(2);
        }
        List<Integer> rates = new ArrayList<>();
        for (String s : o.getOrDefault("rates", "20,40,80,160,320").split(",")) rates.add(Integer.parseInt(s.trim()));
        int step = Integer.parseInt(o.getOrDefault("step", "12"));
        int workers = Integer.parseInt(o.getOrDefault("workers", "2"));
        int concurrency = Integer.parseInt(o.getOrDefault("concurrency",
                String.valueOf(Runtime.getRuntime().availableProcessors())));
        int drain = Integer.parseInt(o.getOrDefault("drain", "20"));

        Blueprint<Map<String, Object>> bp = Workflow.define("loadflow").step("work", c -> c).build();
        AtomicReference<Rec> rec = new AtomicReference<>(new Rec(1));
        ActivityHandler handler = ctx -> {
            Object t0 = ((Map<?, ?>) ctx).get("t0");
            if (t0 instanceof Number) rec.get().record(System.currentTimeMillis() - ((Number) t0).longValue());
            return ctx;
        };

        WorkerOptions opts = WorkerOptions.defaults().withConcurrency(concurrency)
                .withAwaitRegistration(Duration.ofSeconds(20));   // tolerate register/worker start races
        List<AutoCloseable> closes = new ArrayList<>();
        final Runnable startOne;

        if (server != null) {
            try (WiggleClient reg = new WiggleClient(server)) { reg.register(bp); }
            for (int i = 0; i < workers; i++) {
                Worker w = new Worker(new WiggleClient(server), "load-" + i, opts);
                w.handle("loadflow", "work", handler);   // bind handlers, then start
                w.start();
                closes.add(w);
            }
            WiggleClient starter = new WiggleClient(server); closes.add(starter);
            startOne = () -> starter.start("loadflow", ctx());
        } else {
            CellResolver resolver = CellResolver.coordinator(coord, Tls.Options.DISABLED, "");
            resolver.registerWorkflow(ns, bp);
            NamespaceWorker nw = new NamespaceWorker(() -> resolver.activeCellTargets(ns), WiggleClient::new,
                    "load", opts, w -> w.handle("loadflow", "work", handler));
            nw.reconcileEvery(Duration.ofSeconds(3)).start();
            closes.add(nw); closes.add(resolver);
            // Spread new starts across the namespace's active cells (round-robin), so a multi-cell
            // namespace actually uses every cell's database instead of piling onto ring[0].
            List<String> targets = List.of();
            for (int i = 0; i < 20 && targets.isEmpty(); i++) {
                targets = resolver.activeCellTargets(ns);
                if (targets.isEmpty()) Thread.sleep(500);
            }
            if (targets.isEmpty()) { System.err.println("no active cells for '" + ns + "'"); System.exit(1); }
            List<WiggleClient> starters = new ArrayList<>();
            for (String t : targets) { WiggleClient c = new WiggleClient(t); starters.add(c); closes.add(c); }
            System.out.println("starting across " + targets.size() + " cell(s): " + targets);
            AtomicInteger rr = new AtomicInteger();
            startOne = () -> starters.get(Math.floorMod(rr.getAndIncrement(), starters.size())).start("loadflow", ctx());
        }

        String target = server != null ? server : coord + " / " + ns;
        System.out.printf("target=%s  workers=%d  concurrency=%d  step=%ds%n", target, workers, concurrency, step);
        System.out.println("latency = client start() -> step executed on a worker (end-to-end), milliseconds");
        System.out.printf("%6s %8s %8s %7s %7s %7s %7s %9s%n",
                "rate", "start/s", "done/s", "p50", "p95", "p99", "max", "backlog");

        ExecutorService submit = new ThreadPoolExecutor(32, 32, 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(50_000), new ThreadPoolExecutor.CallerRunsPolicy());
        Thread.sleep(1500);   // let workers converge on the cell(s)

        for (int rate : rates) {
            Rec r = new Rec(rate * step * 2); rec.set(r);
            AtomicLong started = new AtomicLong();
            long end = System.nanoTime() + step * 1_000_000_000L;
            long interval = 1_000_000_000L / Math.max(1, rate);
            long next = System.nanoTime();
            while (System.nanoTime() < end) {
                submit.execute(() -> { try { startOne.run(); started.incrementAndGet(); } catch (RuntimeException ignore) {} });
                next += interval;
                long park = next - System.nanoTime();
                if (park > 0) LockSupport.parkNanos(park);
            }
            long doneInWindow = r.count();
            long deadline = System.nanoTime() + drain * 1_000_000_000L;
            while (System.nanoTime() < deadline && r.count() < started.get()) Thread.sleep(200);
            long backlog = Math.max(0, started.get() - r.count());
            System.out.printf("%6d %8.1f %8.1f %7d %7d %7d %7d %9d%n",
                    rate, started.get() / (double) step, doneInWindow / (double) step,
                    r.pct(50), r.pct(95), r.pct(99), r.max(), backlog);
            if (backlog > (long) rate * step * 0.25) {
                System.out.println("  -> overloaded (backlog growing); stopping ramp");
                break;
            }
            Thread.sleep(500);
        }

        submit.shutdownNow();
        for (AutoCloseable c : closes) try { c.close(); } catch (Exception ignore) {}
        System.out.println("done");
    }

    private static Map<String, Object> ctx() {
        Map<String, Object> m = new HashMap<>();
        m.put("t0", System.currentTimeMillis());
        return m;
    }

    private static Map<String, String> parse(String[] a) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < a.length; i++) {
            if (a[i].startsWith("--")) {
                String k = a[i].substring(2);
                String v = (i + 1 < a.length && !a[i + 1].startsWith("--")) ? a[++i] : "true";
                m.put(k, v);
            }
        }
        return m;
    }
}
JAVA

javac -cp "$CP" -d "$OUT" "$OUT/LoadTest.java" || { echo "load harness failed to compile" >&2; exit 1; }
exec java -cp "$OUT:$CP" LoadTest "$@"
