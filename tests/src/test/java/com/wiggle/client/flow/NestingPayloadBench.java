package com.wiggle.client.flow;

import com.wiggle.tests.Reports;
import com.wiggle.core.Json;
import com.wiggle.core.ScratchKeys;
import com.wiggle.core.TaskActivation;
import com.wiggle.jdbc.JdbcStorage;
import com.wiggle.postgres.PostgresDialect;
import com.wiggle.server.engine.DefinitionRegistry;
import com.wiggle.server.engine.WorkflowEngine;
import com.wiggle.server.store.InMemoryStorage;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.Storage;
import com.wiggle.tests.TestDb;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * What nesting depth costs in token-payload BYTES, and what those bytes cost in throughput.
 *
 * <p>A scope frame carries a full copy of its scope's view, so a token nested {@code d} deep holds
 * {@code d} views — payload grows linearly with depth, and the bytes an instance writes over its
 * lifetime grow quadratically (a token at depth k carries k views, summed over every level). This
 * measures both, against the in-memory store and (when {@code WIGGLE_TEST_PG_URL} is set) a real
 * PostgreSQL, where the bytes become real row writes.
 *
 * <p>Two topologies:
 * <ul>
 *   <li><b>spine</b> — forks nested d deep. Every level pushes a frame holding a copy of the
 *       enclosing view, so payload is exactly {@code d x view}: the isolated frame-copy cost.</li>
 *   <li><b>mix</b> — fork / forEach / doWhile cycling down, the realistic shape (doWhile pushes no
 *       frame, so ~2/3 of levels cost a frame).</li>
 * </ul>
 *
 * <p>Opt-in, because it is a load test:
 * <pre>
 *   WIGGLE_BENCH_NESTING=1 ./gradlew :tests:test --tests '*NestingPayloadBench*'
 *   WIGGLE_BENCH_NESTING=1 WIGGLE_TEST_PG_URL=jdbc:postgresql://localhost:5433/wiggle \
 *     WIGGLE_TEST_PG_USER=wiggle WIGGLE_TEST_PG_PASSWORD=wiggle ./gradlew :tests:test --tests '*NestingPayloadBench*'
 * </pre>
 * Tunables: {@code BENCH_DEPTHS} (1,5,10,25,50,100), {@code BENCH_BALLAST} (context filler bytes;
 * 0,256,1024), {@code BENCH_INSTANCES} (20), {@code BENCH_THREADS} (8).
 */
@EnabledIfEnvironmentVariable(named = "WIGGLE_BENCH_NESTING", matches = ".+")
class NestingPayloadBench {

    private static final String LEAF = "leaf";

    /** One config's measurements. {@code dbDelta} is the on-disk growth of the token table
     *  (TOAST and indexes included), or -1 when running in memory. */
    private record Result(String topology, int depth, int ballast, int instances,
                          long tokens, long peakPayload, long storedBytes, double seconds,
                          long completions, long dbDelta) {
        double instancesPerSec() { return instances / seconds; }
        double tasksPerSec() { return completions / seconds; }
        double tokensPerInstance() { return (double) tokens / instances; }
        double bytesPerInstance() { return (double) storedBytes / instances; }
        double dbBytesPerInstance() { return (double) dbDelta / instances; }
    }

    @Test @DisplayName("payload bytes and throughput across nesting depth")
    void sweep() throws Exception {
        List<Integer> depths = ints(env("BENCH_DEPTHS", "1,5,10,25,50,100"));
        List<Integer> ballasts = ints(env("BENCH_BALLAST", "0,256,1024"));
        int instances = Integer.parseInt(env("BENCH_INSTANCES", "20"));
        int threads = Integer.parseInt(env("BENCH_THREADS", "8"));
        String pg = System.getenv("WIGGLE_TEST_PG_URL");

        System.out.printf("%nnesting payload bench: store=%s instances=%d threads=%d%n",
                pg == null ? "in-memory" : "postgres", instances, threads);

        List<Result> results = new ArrayList<>();
        for (String topology : List.of("spine", "mix")) {
            for (int depth : depths) {
                for (int ballast : ballasts) {
                    results.add(run(topology, depth, ballast, instances, threads, pg));
                }
            }
        }
        report(results, pg);
    }

    private Result run(String topology, int depth, int ballast, int instances, int threads, String pg)
            throws Exception {
        try (Storage storage = open(pg)) {
            storage.migrate();
            DefinitionRegistry registry = new DefinitionRegistry(storage);
            WorkflowEngine engine = new WorkflowEngine(storage, registry, 120_000);
            FlowSpec bp = build(topology, depth);
            registry.register(bp.definition());
            Set<String> queues = bp.definition().queues();

            Object input = inputFor(topology, depth, ballast);
            long dbBefore = resetTokenTable(pg);
            List<String> ids = new ArrayList<>(instances);
            for (int i = 0; i < instances; i++) {
                ids.add(engine.start(bp.name(), bp.version(), input, null));
            }

            AtomicLong completions = new AtomicLong();
            long t0 = System.nanoTime();
            drive(engine, queues, ids, threads, completions);
            double seconds = (System.nanoTime() - t0) / 1e9;

            long tokens = 0, peak = 0, stored = 0;
            for (String id : ids) {
                for (Rows.Token t : engine.tokens(id)) {
                    // the stored form is what costs: measure the encoded payload, not the object
                    String encoded = com.wiggle.server.store.PayloadCodec.encode(t.payload);
                    int len = encoded == null ? 0 : encoded.length();
                    tokens++;
                    stored += len;
                    peak = Math.max(peak, len);
                }
                if (!engine.instance(id).orElseThrow().status().equals("COMPLETED")) {
                    throw new AssertionError(topology + " d=" + depth + " instance " + id + " not COMPLETED");
                }
            }
            long dbDelta = dbBefore < 0 ? -1 : Math.max(0, tokenTableBytes(pg) - dbBefore);
            return new Result(topology, depth, ballast, instances, tokens, peak, stored, seconds,
                    completions.get(), dbDelta);
        }
    }

    /** Empties the token table so this config's on-disk size is measured from a clean floor --
     *  a plain before/after delta is meaningless here, because PostgreSQL reuses space freed by
     *  the previous config and the delta then understates (or hides) the real cost. */
    private static long resetTokenTable(String pg) throws Exception {
        if (pg == null) return -1;
        try (java.sql.Connection c = java.sql.DriverManager.getConnection(
                     TestDb.url("PG"), TestDb.user("PG"), TestDb.password("PG"));
             java.sql.Statement s = c.createStatement()) {
            s.execute("TRUNCATE wf_token");
            return tokenTableBytes(s);
        }
    }

    /** On-disk size of the token table including TOAST and indexes, or -1 when in memory. TOAST
     *  compresses a payload past ~2KB, so this is what depth actually costs the database. */
    private static long tokenTableBytes(String pg) throws Exception {
        if (pg == null) return -1;
        try (java.sql.Connection c = java.sql.DriverManager.getConnection(
                     TestDb.url("PG"), TestDb.user("PG"), TestDb.password("PG"));
             java.sql.Statement s = c.createStatement()) {
            return tokenTableBytes(s);
        }
    }

    private static long tokenTableBytes(java.sql.Statement s) throws Exception {
        s.execute("VACUUM wf_token");   // settle dead tuples so the size reflects live data
        try (java.sql.ResultSet rs = s.executeQuery("SELECT pg_total_relation_size('wf_token')")) {
            return rs.next() ? rs.getLong(1) : -1;
        }
    }

    /** Polls and completes until every instance is terminal. */
    private void drive(WorkflowEngine engine, Set<String> queues, List<String> ids, int threads,
                       AtomicLong completions) throws Exception {
        ConcurrentLinkedQueue<Exception> failures = new ConcurrentLinkedQueue<>();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch done = new CountDownLatch(threads);
        long deadline = System.nanoTime() + java.time.Duration.ofMinutes(10).toNanos();
        for (int i = 0; i < threads; i++) {
            String worker = "bench-w" + i;
            pool.execute(() -> {
                try {
                    while (System.nanoTime() < deadline) {
                        List<TaskActivation> batch = engine.poll(worker, queues, 32, null);
                        if (batch.isEmpty()) {
                            if (allTerminal(engine, ids)) return;
                            Thread.sleep(2);
                            continue;
                        }
                        for (TaskActivation t : batch) {
                            Reports.one(engine, t, worker, resultFor(t));
                            completions.incrementAndGet();
                        }
                    }
                    failures.add(new IllegalStateException("bench did not drain within 10 minutes"));
                } catch (Exception e) {
                    failures.add(e);
                } finally {
                    done.countDown();
                }
            });
        }
        done.await();
        pool.shutdownNow();
        if (!failures.isEmpty()) throw failures.peek();
    }

    private static boolean allTerminal(WorkflowEngine engine, List<String> ids) {
        for (String id : ids) {
            if (engine.instance(id).orElseThrow().status().equals("RUNNING")) return false;
        }
        return true;
    }

    // ---------- topologies ----------

    private static FlowSpec build(String topology, int depth) {
        GraphBuilder b = Workflow.define(topology + "-d" + depth + "-" + com.wiggle.core.Ids.next("wf"), 1);
        return (topology.equals("spine") ? spine(b, depth) : mix(b, depth)).build();
    }

    /** Forks nested d deep: every level pushes one arm frame holding a copy of the enclosing view. */
    private static GraphBuilder spine(GraphBuilder b, int d) {
        if (d == 0) return b.then(LEAF);
        return b.fork(Branch.of("deep" + d, inner -> spine(inner, d - 1)),
                        Branch.of("side" + d, inner -> inner.then("sd" + d)))
                .combine("csp" + d);
    }

    /** fork / forEach / doWhile cycling down -- the realistic shape. */
    private static GraphBuilder mix(GraphBuilder b, int d) {
        if (d == 0) return b.then(LEAF);
        return switch (d % 3) {
            case 1 -> b.fork(Branch.of("deep" + d, inner -> mix(inner, d - 1)),
                            Branch.of("side" + d, inner -> inner.then("sd" + d)))
                    .combine("cfk" + d);
            case 0 -> b.forEach("fe" + d, "items", inner -> mix(inner, d - 1)).combine("cfe" + d);
            default -> b.doWhile("g" + d, 5, inner -> mix(inner, d - 1));
        };
    }

    /** The starting context: ballast filler at the innermost level, wrapped by one {"items":[...]}
     *  per forEach level so each fan-out finds its collection in the enclosing view.
     *
     *  <p>{@code BENCH_BALLAST_KIND} decides how compressible that filler is, which matters a great
     *  deal on a real database: PostgreSQL TOASTs (compresses) any payload past ~2KB, and a scope
     *  stack is inherently repetitive — every frame holds a near-copy of its parent's view. {@code
     *  repeat} (a run of one character) is the optimistic bound, {@code random} the pessimistic one;
     *  real JSON contexts sit between. */
    private static Object inputFor(String topology, int d, int ballast) {
        Map<String, Object> leaf = new LinkedHashMap<>();
        leaf.put("hit", 0L);
        if (ballast > 0) leaf.put("ballast", ballast(ballast));
        if (topology.equals("spine")) return leaf;
        Object view = leaf;
        for (int level = 1; level <= d; level++) {
            if (level % 3 == 0) view = new LinkedHashMap<>(Map.of("items", List.of(view)));
        }
        return view;
    }

    /** Filler of {@code n} characters, as compressible as {@code BENCH_BALLAST_KIND} asks for.
     *  Seeded, so a rerun of the same config measures the same bytes. */
    private static String ballast(int n) {
        if (!env("BENCH_BALLAST_KIND", "repeat").equals("random")) return "x".repeat(n);
        java.util.Random rnd = new java.util.Random(n);
        StringBuilder sb = new StringBuilder(n);
        String alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        for (int i = 0; i < n; i++) sb.append(alphabet.charAt(rnd.nextInt(alphabet.length())));
        return sb.toString();
    }

    /** What a worker would return for each step, shape-preserving so every level rebuilds its view. */
    private static Object resultFor(TaskActivation t) {
        String a = t.activity().substring(t.activity().indexOf('#') + 1);
        if (a.equals(LEAF)) return t.context();                  // identity: the view passes through
        if (a.startsWith("sd")) return null;                     // side arm: view untouched
        if (a.startsWith("csp") || a.startsWith("cfk")) {        // fork combine: the deep arm wins
            String d = a.substring(3);
            return Json.asObject(t.context()).get(ScratchKeys.arm("deep" + d));
        }
        if (a.startsWith("cfe")) {                               // forEach combine: rebuild the wrapper
            Object collected = Json.asObject(t.context()).get(ScratchKeys.forEach("fe" + a.substring(3)));
            return Map.of("items", List.of(Json.asArray(collected).getFirst()));
        }
        if (a.startsWith("g")) return false;                     // loop guards: one pass (see class doc)
        throw new AssertionError("unexpected activity " + a);
    }

    // ---------- reporting ----------

    private static void report(List<Result> results, String pg) {
        System.out.printf(java.util.Locale.ROOT, "%n%-7s %6s %8s %9s %13s %13s %12s %10s %10s%n",
                "shape", "depth", "ballast", "tokens/i", "peak payload", "stored/inst", "db/inst",
                "inst/s", "tasks/s");
        System.out.println("-".repeat(97));
        for (Result r : results) {
            System.out.printf(java.util.Locale.ROOT, "%-7s %6d %8d %9.0f %13s %13s %12s %10.1f %10.0f%n",
                    r.topology(), r.depth(), r.ballast(), r.tokensPerInstance(),
                    bytes(r.peakPayload()), bytes((long) r.bytesPerInstance()),
                    r.dbDelta() < 0 ? "-" : bytes((long) r.dbBytesPerInstance()),
                    r.instancesPerSec(), r.tasksPerSec());
        }
        System.out.printf(java.util.Locale.ROOT, "%nstore=%s%n", pg == null ? "in-memory" : "postgres");
    }

    private static String bytes(long n) {
        if (n < 1024) return n + " B";
        if (n < 1024 * 1024) return String.format(java.util.Locale.ROOT, "%.1f KB", n / 1024.0);
        return String.format(java.util.Locale.ROOT, "%.1f MB", n / (1024.0 * 1024));
    }

    // ---------- plumbing ----------

    private static Storage open(String pg) {
        if (pg == null) return new InMemoryStorage();
        return new JdbcStorage(TestDb.url("PG"), TestDb.user("PG"), TestDb.password("PG"), 16,
                new PostgresDialect());
    }

    private static List<Integer> ints(String csv) {
        return java.util.Arrays.stream(csv.split(",")).map(String::trim).map(Integer::parseInt).toList();
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? def : v;
    }
}
