package com.wiggle.postgres;

import com.wiggle.jdbc.JdbcStorage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** The versioned schema runner: fresh bootstrap, incremental change, and idempotent re-runs. */
class SchemaMigrationTest {

    private static int schemaVersion(Connection c) throws Exception {
        try (Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT COALESCE(MAX(version),0) FROM wf_schema_version")) {
            return rs.next() ? rs.getInt(1) : 0;
        }
    }

    private static int rowCount(Connection c, String table) throws Exception {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + table)) {
            rs.next();
            return rs.getInt(1);
        }
    }

    /** The version of the last real migration -- what a fresh database should land on. */
    private static int baselineVersion() {
        return JdbcStorage.MIGRATIONS.get(JdbcStorage.MIGRATIONS.size() - 1).version();
    }

    @Test @DisplayName("bootstraps a fresh database to the latest recorded version")
    void bootstrap() throws Exception {
        String url = "jdbc:h2:mem:mig-boot-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
        try (JdbcStorage storage = new JdbcStorage(url, "sa", "", 2, new H2Dialect())) {
            storage.migrate();
            try (Connection c = DriverManager.getConnection(url, "sa", "")) {
                assertEquals(baselineVersion(), schemaVersion(c), "every real migration applied");
                // A baseline table is usable.
                assertDoesNotThrow(() -> c.createStatement().executeQuery("SELECT * FROM wf_token"));
            }
        }
    }

    @Test @DisplayName("applies a new migration once, evolving an existing table, and is idempotent")
    void incrementalAndIdempotent() throws Exception {
        String url = "jdbc:h2:mem:mig-inc-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
        try (JdbcStorage storage = new JdbcStorage(url, "sa", "", 2, new H2Dialect())) {
            storage.migrate();   // every real migration up to baselineVersion()
            int next = baselineVersion() + 1;

            // Simulate the history growing: MIGRATIONS + one more that ALTERs an existing table.
            List<JdbcStorage.Migration> withNext = new ArrayList<>(JdbcStorage.MIGRATIONS);
            withNext.add(new JdbcStorage.Migration(next, "add-node-region",
                    "ALTER TABLE wf_node ADD COLUMN region VARCHAR(50)"));

            try (Connection c = DriverManager.getConnection(url, "sa", "")) {
                c.setAutoCommit(false);

                JdbcStorage.runMigrations(c, withNext, new H2Dialect());
                c.commit();
                assertEquals(next, schemaVersion(c), "the new migration applied");
                assertEquals(next, rowCount(c, "wf_schema_version"), "one row per applied migration");
                assertDoesNotThrow(() -> c.createStatement().executeQuery("SELECT region FROM wf_node"),
                        "the added column exists");

                // Re-running is a no-op: every version is already recorded, so nothing re-executes
                // (a second ALTER would fail with 'column already exists').
                JdbcStorage.runMigrations(c, withNext, new H2Dialect());
                c.commit();
                assertEquals(next, schemaVersion(c), "still at the same version");
                assertEquals(next, rowCount(c, "wf_schema_version"), "no duplicate history rows");
            }
        }
    }

    @Test @DisplayName("a failing migration rolls back and records nothing (Postgres-style atomicity)")
    void failingMigrationLeavesNoTrace() throws Exception {
        String url = "jdbc:h2:mem:mig-fail-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
        try (JdbcStorage storage = new JdbcStorage(url, "sa", "", 2, new H2Dialect())) {
            storage.migrate();
            int baseline = baselineVersion();

            List<JdbcStorage.Migration> bad = new ArrayList<>(JdbcStorage.MIGRATIONS);
            bad.add(new JdbcStorage.Migration(baseline + 1, "broken", "ALTER TABLE does_not_exist ADD COLUMN x INT"));

            try (Connection c = DriverManager.getConnection(url, "sa", "")) {
                c.setAutoCommit(false);
                assertThrows(Exception.class, () -> JdbcStorage.runMigrations(c, bad, new H2Dialect()));
                c.rollback();
                assertEquals(baseline, schemaVersion(c), "failed migration was not recorded");
            }
        }
    }
}
