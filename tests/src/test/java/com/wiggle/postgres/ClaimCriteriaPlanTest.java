package com.wiggle.postgres;

import com.wiggle.core.criteria.CriteriaBuilder;
import com.wiggle.core.criteria.Predicate;
import com.wiggle.core.criteria.QueueCriteria;
import com.wiggle.core.criteria.Task_;
import com.wiggle.jdbc.JdbcStorage;
import com.wiggle.server.store.ClaimFilter;
import com.wiggle.server.store.Rows.Token;
import com.wiggle.tests.TestDb;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * WGL-QC-100: the plan PostgreSQL chooses for a claim narrowed by criteria, on one deep queue of
 * {@value #BACKLOG} ready tasks in instance-age order. 1% are {@code region = eu} and carry an
 * {@code eu-} correlation id, spread evenly; the newest 1% alone are {@code tier = gold}. A rare
 * attribute value must be found through its value index rather than by reading every attribute
 * row, and no claim may scan a whole table. Each claim is also run, rolled back, for what it takes.
 * The plans depend on the backlog: at 10^5 tasks a task-field predicate is read by a scan of
 * {@code wf_token}, which the planner prefers to walking the index for a {@code LIKE} it expects to
 * match a handful of rows. Loading the backlog takes about 20 seconds.
 * Opt-in, as {@link PostgresClaimTest}:
 *
 * <pre>
 *   WIGGLE_TEST_PG_URL=jdbc:postgresql://localhost:5433/wiggle \
 *   WIGGLE_TEST_PG_USER=wiggle WIGGLE_TEST_PG_PASSWORD=wiggle \
 *     ./gradlew :tests:test --tests "com.wiggle.postgres.ClaimCriteriaPlanTest"
 * </pre>
 */
@EnabledIfEnvironmentVariable(named = "WIGGLE_TEST_PG_URL", matches = ".+")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ClaimCriteriaPlanTest {

    private static final String QUEUE = "qc-plan";
    private static final int BACKLOG = 1_000_000;
    private static final int MAX = 10;

    private JdbcStorage storage;

    @BeforeAll void load() throws SQLException {
        storage = new JdbcStorage(TestDb.url("PG"), TestDb.user("PG"), TestDb.password("PG"), 2, new PostgresDialect());
        storage.migrate();
        try (Connection c = DriverManager.getConnection(TestDb.url("PG"), TestDb.user("PG"), TestDb.password("PG"));
             Statement st = c.createStatement()) {
            c.setAutoCommit(true);
            st.execute("TRUNCATE wf_token, wf_token_attr");
            st.execute("INSERT INTO wf_token (id,instance_id,workflow,version,node_id,kind,status,activity,queue,"
                    + "attempt,available_at,lease_expires,join_stack,created_at,updated_at,inst_created_at,correlation_id)"
                    + " SELECT 'p'||i, 'wfi-p'||i, 'plan', 1, 'n1', 'TASK', 'READY', 'plan#step', '" + QUEUE + "',"
                    + " 0, 0, 0, '', 0, 0, i, CASE WHEN i % 100 = 0 THEN 'eu-' ELSE 'us-' END || i"
                    + " FROM generate_series(1," + BACKLOG + ") i");
            st.execute("INSERT INTO wf_token_attr (token_id,name,t,s_val)"
                    + " SELECT 'p'||i, 'region', 'S', CASE WHEN i % 100 = 0 THEN 'eu' ELSE 'us' END"
                    + " FROM generate_series(1," + BACKLOG + ") i");
            st.execute("INSERT INTO wf_token_attr (token_id,name,t,s_val)"
                    + " SELECT 'p'||i, 'tier', 'S', 'gold' FROM generate_series(" + (BACKLOG - BACKLOG / 100 + 1)
                    + "," + BACKLOG + ") i");
            st.execute("VACUUM ANALYZE wf_token");
            st.execute("VACUUM ANALYZE wf_token_attr");
        }
    }

    @AfterAll void close() {
        storage.close();
    }

    @Test @DisplayName("queue name only: the queue's ready index, in age order")
    void queueOnly() {
        String plan = plan(null, MAX);
        assertUses(plan, "ix_token_ready_age");
        assertNoSeqScan(plan);
    }

    @Test @DisplayName("a task field held by 1%: the queue's ready index, filtered")
    void tokenColumnCommon() {
        String plan = plan(where(cb -> cb.like(Task_.CORRELATION_ID, "eu-%")), MAX);
        assertUses(plan, "ix_token_ready_age");
        assertNoSeqScan(plan);
    }

    @Test @DisplayName("a task field held by none: still the queue's index, never a table scan")
    void tokenColumnNoMatch() {
        String plan = plan(where(cb -> cb.like(Task_.CORRELATION_ID, "apac-%")), 0);
        assertUses(plan, "ix_token_ready_age");
        assertNoSeqScan(plan);
    }

    @Test @DisplayName("an attribute held by 1%: no scan of every attribute row")
    void attributeCommon() {
        assertNoSeqScan(plan(where(cb -> cb.equal(Task_.attr("region", String.class), "eu")), MAX));
    }

    @Test @DisplayName("an attribute only the newest 1% hold: found through the value index")
    void attributeNewest() {
        Predicate gold = where(cb -> cb.equal(Task_.attr("tier", String.class), "gold"));
        for (Token t : claim(gold)) assertTrue(t.instCreatedAt > BACKLOG - BACKLOG / 100, "claimed " + t.id);
        String plan = plan(gold, MAX);
        assertUses(plan, "ix_token_attr_s");
        assertNoSeqScan(plan);
    }

    @Test @DisplayName("an attribute value no task holds: found through the value index")
    void attributeNoMatch() {
        String plan = plan(where(cb -> cb.equal(Task_.attr("region", String.class), "apac")), 0);
        assertUses(plan, "ix_token_attr_s");
        assertNoSeqScan(plan);
    }

    /** The claim's plan for {@code where}, after checking a rolled-back claim takes {@code expected}. */
    private String plan(Predicate where, int expected) {
        assertEquals(expected, claim(where).size(), "tasks claimed");
        return storage.explainClaim(new ClaimFilter(Set.of(QUEUE), null, where), MAX, System.currentTimeMillis());
    }

    /** What a claim for {@code where} takes, rolled back so every test sees the same backlog. */
    private List<Token> claim(Predicate where) {
        ClaimFilter filter = new ClaimFilter(Set.of(QUEUE), null, where);
        long now = System.currentTimeMillis();
        try {
            storage.inTx(tx -> {
                throw new Rolled(tx.claimTasks("plan", filter, MAX, now, now + 60_000));
            });
        } catch (RuntimeException e) {
            for (Throwable x = e; x != null; x = x.getCause()) if (x instanceof Rolled r) return r.claimed;
            throw e;
        }
        throw new AssertionError("unreachable");
    }

    private static final class Rolled extends RuntimeException {
        final List<Token> claimed;

        Rolled(List<Token> claimed) {
            super(null, null, false, false);
            this.claimed = claimed;
        }
    }

    private static void assertUses(String plan, String index) {
        assertTrue(plan.contains(" " + index), "uses " + index + ":\n" + plan);
    }

    private static void assertNoSeqScan(String plan) {
        assertFalse(plan.contains("Seq Scan"), "no table is read whole:\n" + plan);
    }

    private static Predicate where(Function<CriteriaBuilder, Predicate> p) {
        return QueueCriteria.from(QUEUE).where(p).where();
    }
}
