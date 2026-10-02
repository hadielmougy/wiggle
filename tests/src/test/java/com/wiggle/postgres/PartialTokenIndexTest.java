package com.wiggle.postgres;

import com.wiggle.jdbc.JdbcStorage;
import com.wiggle.tests.TestDb;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Migration 19: on PostgreSQL wf_token is indexed per status, and every status-filtered query the
 * store runs can use its partial index; on H2, which has no partial indexes, the migration is
 * recorded and the full indexes stay.
 */
class PartialTokenIndexTest {

    private static final Set<String> PARTIAL = Set.of("ix_token_ready", "ix_token_waiting", "ix_token_awaiting",
            "ix_token_running", "ix_token_done", "ix_token_done_timed");
    private static final Set<String> REPLACED = Set.of("ix_token_dispatch", "ix_token_lease",
            "ix_token_throughput", "ix_token_timed");

    private static Set<String> tokenIndexes(Connection c) throws Exception {
        Set<String> names = new HashSet<>();
        for (String table : new String[]{"wf_token", "WF_TOKEN"}) {
            try (ResultSet rs = c.getMetaData().getIndexInfo(null, null, table, false, false)) {
                while (rs.next()) {
                    String name = rs.getString("INDEX_NAME");
                    if (name != null) names.add(name.toLowerCase(Locale.ROOT));
                }
            }
        }
        return names;
    }

    private static int schemaVersion(Connection c) throws Exception {
        try (Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT MAX(version) FROM wf_schema_version")) {
            rs.next();
            return rs.getInt(1);
        }
    }

    @Test @DisplayName("H2 records the migration and keeps the full indexes")
    void h2KeepsFullIndexes() throws Exception {
        String url = "jdbc:h2:mem:partial-idx-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
        try (JdbcStorage storage = new JdbcStorage(url, "sa", "", 2, new H2Dialect())) {
            storage.migrate();
            try (Connection c = DriverManager.getConnection(url, "sa", "")) {
                assertTrue(schemaVersion(c) >= 19, "the migration is recorded as applied");
                Set<String> indexes = tokenIndexes(c);
                assertTrue(indexes.containsAll(REPLACED), "full indexes kept: " + indexes);
                assertTrue(indexes.stream().noneMatch(PARTIAL::contains), "no partial index created: " + indexes);
            }
        }
    }

    private static final Set<String> READY = Set.of("ix_token_ready");
    private static final Set<String> DONE = Set.of("ix_token_done", "ix_token_done_timed");

    /** Each query, as the store issues it, against the partial indexes over its status. Which of
     *  two indexes over the same status the planner picks depends on the table's statistics. */
    private static final Map<String, Set<String>> PLANS = new HashMap<>();
    static {
        PLANS.put("SELECT id FROM wf_token WHERE status='READY' AND kind IN ('TASK','PREDICATE')"
                + " AND available_at<=1000 AND queue IN ('q1','q2') ORDER BY available_at, id LIMIT 10",
                READY);
        PLANS.put("SELECT COUNT(*), COALESCE(MIN(available_at),0) FROM wf_token WHERE status='READY'"
                + " AND kind IN ('TASK','PREDICATE') AND available_at<=1000", READY);
        PLANS.put("SELECT * FROM wf_token WHERE status='WAITING' AND kind='SLEEP' AND available_at<=1000"
                + " ORDER BY available_at LIMIT 10", Set.of("ix_token_waiting"));
        PLANS.put("SELECT * FROM wf_token WHERE status='AWAITING' AND kind='SIGNAL' AND available_at>0"
                + " AND available_at<=1000 ORDER BY available_at LIMIT 10", Set.of("ix_token_awaiting"));
        PLANS.put("SELECT * FROM wf_token WHERE status='RUNNING' AND lease_expires>0 AND lease_expires<1000"
                + " ORDER BY lease_expires LIMIT 10", Set.of("ix_token_running"));
        PLANS.put("SELECT COUNT(*) FROM wf_token WHERE kind IN ('TASK','PREDICATE') AND status='DONE'"
                + " AND updated_at>1000", DONE);
        PLANS.put("SELECT node_id, started_at, finished_at, available_at, seq FROM wf_token WHERE workflow='w'"
                + " AND version=1 AND status='DONE' AND finished_at > 1000 AND started_at IS NOT NULL"
                + " ORDER BY finished_at DESC LIMIT 10", DONE);
    }

    @Test @DisplayName("PostgreSQL indexes wf_token per status, and each status query uses an index over its status")
    @EnabledIfEnvironmentVariable(named = "WIGGLE_TEST_PG_URL", matches = ".+")
    void postgresUsesPartialIndexes() throws Exception {
        String url = TestDb.url("PG");
        try (JdbcStorage storage = new JdbcStorage(url, TestDb.user("PG"), TestDb.password("PG"), 2,
                new PostgresDialect())) {
            storage.migrate();
        }
        try (Connection c = DriverManager.getConnection(url, TestDb.user("PG"), TestDb.password("PG"))) {
            Set<String> indexes = tokenIndexes(c);
            assertTrue(indexes.containsAll(PARTIAL), "partial indexes created: " + indexes);
            assertTrue(indexes.stream().noneMatch(REPLACED::contains), "full indexes dropped: " + indexes);

            try (Statement st = c.createStatement()) {
                // A test table is small enough that a sequential scan always wins; take it off the
                // table so the plan shows which index the planner can match.
                st.execute("SET enable_seqscan = off");
                for (Map.Entry<String, Set<String>> e : PLANS.entrySet()) {
                    StringBuilder plan = new StringBuilder();
                    try (ResultSet rs = st.executeQuery("EXPLAIN " + e.getKey())) {
                        while (rs.next()) plan.append(rs.getString(1)).append('\n');
                    }
                    assertTrue(e.getValue().stream().anyMatch(ix -> plan.toString().contains(ix + " ")),
                            "expected one of " + e.getValue() + " for: " + e.getKey() + "\n" + plan);
                }
            }
        }
    }
}
