package com.wiggle.postgres;

import com.wiggle.core.criteria.Predicate;
import com.wiggle.core.criteria.QueueCriteria;
import com.wiggle.core.criteria.Task_;
import com.wiggle.jdbc.JdbcStorage;
import com.wiggle.server.store.ClaimFilter;
import com.wiggle.server.store.Rows.Token;
import com.wiggle.server.store.Storage;
import com.wiggle.tests.TestDb;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * WGL-QC-100: what a narrow criteria predicate costs the claim on a deep queue. Each size loads one
 * queue of N ready tasks in instance-age order: 1% are {@code region = eu} and carry an {@code eu-}
 * correlation id, spread evenly; the newest 1% alone are {@code tier = gold}. Every claim takes up
 * to 10 and is rolled back, so each sample sees the same backlog.
 *
 * <pre>
 *   WIGGLE_BENCH_CLAIM_CRITERIA=1 \
 *   WIGGLE_TEST_PG_URL=jdbc:postgresql://localhost:5433/wiggle \
 *   WIGGLE_TEST_PG_USER=wiggle WIGGLE_TEST_PG_PASSWORD=wiggle \
 *     ./gradlew :tests:test --tests "*ClaimCriteriaBench*" -i
 * </pre>
 *
 * {@code WIGGLE_BENCH_SIZES} overrides the backlog sizes (default 10000,100000,1000000).
 */
@EnabledIfEnvironmentVariable(named = "WIGGLE_BENCH_CLAIM_CRITERIA", matches = ".+")
@EnabledIfEnvironmentVariable(named = "WIGGLE_TEST_PG_URL", matches = ".+")
class ClaimCriteriaBench {

    private static final String QUEUE = "qc-bench";
    private static final int MAX = 10;
    private static final int ROUNDS = 3;
    private static final int SAMPLES = 20;

    private record Variant(String name, Predicate where, int expected) { }

    private static final class Rolled extends RuntimeException {
        final List<Token> claimed;

        Rolled(List<Token> claimed) {
            super(null, null, false, false);
            this.claimed = claimed;
        }
    }

    @Test
    @DisplayName("WGL-QC-100: claim latency of narrow predicates on deep queues")
    void narrowPredicatesOnDeepQueues() throws Exception {
        String sizes = System.getenv().getOrDefault("WIGGLE_BENCH_SIZES", "10000,100000,1000000");
        List<Variant> variants = List.of(
                new Variant("queue name only", null, MAX),
                new Variant("token column, 1% even", where(cb -> cb.like(Task_.CORRELATION_ID, "eu-%")), MAX),
                new Variant("token column, no match", where(cb -> cb.like(Task_.CORRELATION_ID, "apac-%")), 0),
                new Variant("attribute, 1% even", where(cb -> cb.equal(Task_.attr("region", String.class), "eu")), MAX),
                new Variant("attribute, newest 1%", where(cb -> cb.equal(Task_.attr("tier", String.class), "gold")), MAX),
                new Variant("attribute, no match", where(cb -> cb.equal(Task_.attr("region", String.class), "apac")), 0));

        try (Storage storage = new JdbcStorage(TestDb.url("PG"), TestDb.user("PG"), TestDb.password("PG"), 4,
                new PostgresDialect());
             Connection c = DriverManager.getConnection(TestDb.url("PG"), TestDb.user("PG"), TestDb.password("PG"))) {
            storage.migrate();
            List<String> out = new ArrayList<>();
            out.add(String.format("%-10s %-24s %10s %10s %10s", "backlog", "claim", "p50 ms", "p95 ms", "max ms"));
            out.add("-".repeat(68));
            for (String s : sizes.split(",")) {
                int n = Integer.parseInt(s.trim());
                long loaded = load(c, n);
                Map<String, List<Double>> samples = new LinkedHashMap<>();
                for (Variant v : variants) {
                    for (int i = 0; i < 5; i++) claim(storage, v, n);   // warmup: plans, caches, JIT
                }
                for (int round = 0; round < ROUNDS; round++) {
                    for (Variant v : variants) {
                        for (int i = 0; i < SAMPLES; i++) {
                            samples.computeIfAbsent(v.name(), k -> new ArrayList<>()).add(claim(storage, v, n));
                        }
                    }
                }
                for (Variant v : variants) {
                    double[] ms = samples.get(v.name()).stream().mapToDouble(Double::doubleValue).sorted().toArray();
                    out.add(String.format("%-10d %-24s %10.2f %10.2f %10.2f", n, v.name(),
                            pct(ms, 50), pct(ms, 95), ms[ms.length - 1]));
                }
                out.add(String.format("%-10s (loaded in %d ms)", "", loaded));
            }
            System.out.println();
            out.forEach(System.out::println);
        }
    }

    private static Predicate where(java.util.function.Function<com.wiggle.core.criteria.CriteriaBuilder, Predicate> p) {
        return QueueCriteria.from(QUEUE).where(p).where();
    }

    /** One rolled-back claim, timed in ms; checks it took what the variant should. */
    private static double claim(Storage storage, Variant v, int n) {
        ClaimFilter filter = new ClaimFilter(java.util.Set.of(QUEUE), null, v.where());
        long now = System.currentTimeMillis();
        long t0 = System.nanoTime();
        List<Token> claimed;
        try {
            storage.inTx(tx -> {
                throw new Rolled(tx.claimTasks("bench", filter, MAX, now, now + 60_000));
            });
            throw new AssertionError("unreachable");
        } catch (RuntimeException e) {
            claimed = rolled(e);
        }
        double ms = (System.nanoTime() - t0) / 1e6;
        assertEquals(v.expected(), claimed.size(), v.name());
        if (v.name().contains("newest")) {
            for (Token t : claimed) assertTrue(t.instCreatedAt > n - n / 100, v.name() + " claimed " + t.id);
        }
        return ms;
    }

    private static List<Token> rolled(Throwable e) {
        for (Throwable x = e; x != null; x = x.getCause()) if (x instanceof Rolled r) return r.claimed;
        throw new AssertionError("claim failed", e);
    }

    /** Replaces the backlog with {@code n} ready tasks and their attributes; returns the load time. */
    private static long load(Connection c, int n) throws SQLException {
        long t0 = System.currentTimeMillis();
        try (Statement st = c.createStatement()) {
            st.execute("TRUNCATE wf_token, wf_token_attr");
            st.execute("INSERT INTO wf_token (id,instance_id,workflow,version,node_id,kind,status,activity,queue,"
                    + "attempt,available_at,lease_expires,join_stack,created_at,updated_at,inst_created_at,correlation_id)"
                    + " SELECT 'b'||i, 'wfi-b'||i, 'bench', 1, 'n1', 'TASK', 'READY', 'bench#step', '" + QUEUE + "',"
                    + " 0, 0, 0, '', 0, 0, i, CASE WHEN i % 100 = 0 THEN 'eu-' ELSE 'us-' END || i"
                    + " FROM generate_series(1," + n + ") i");
            st.execute("INSERT INTO wf_token_attr (token_id,name,t,s_val)"
                    + " SELECT 'b'||i, 'region', 'S', CASE WHEN i % 100 = 0 THEN 'eu' ELSE 'us' END"
                    + " FROM generate_series(1," + n + ") i");
            st.execute("INSERT INTO wf_token_attr (token_id,name,t,s_val)"
                    + " SELECT 'b'||i, 'tier', 'S', 'gold' FROM generate_series(" + (n - n / 100 + 1) + "," + n + ") i");
            st.execute("VACUUM ANALYZE wf_token");
            st.execute("VACUUM ANALYZE wf_token_attr");
        }
        return System.currentTimeMillis() - t0;
    }

    private static double pct(double[] sorted, int p) {
        return sorted[Math.min(sorted.length - 1, (int) Math.ceil(p / 100.0 * sorted.length) - 1)];
    }
}
