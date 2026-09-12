package com.wiggle.tests;

import com.wiggle.jdbc.JdbcStorage;
import com.wiggle.jdbc.JdbcStorage.Migration;
import com.wiggle.jdbc.JdbcStorage.MigrationMode;
import com.wiggle.postgres.H2Dialect;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The migration runner's checksum drift-detection and VERIFY mode (H2, PostgreSQL mode). Exercises the
 * public static {@code runMigrations} directly with controlled migration lists.
 */
class JdbcMigrationTest {

    private static Connection h2() throws SQLException {
        Connection c = DriverManager.getConnection(
                "jdbc:h2:mem:mig-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1", "sa", "");
        c.setAutoCommit(false);
        return c;
    }

    private static Migration m(int v, String sql) { return new Migration(v, "v" + v, sql); }

    private static void run(Connection c, List<Migration> migs, MigrationMode mode) throws SQLException {
        JdbcStorage.runMigrations(c, migs, new H2Dialect(), "v1", mode);
        c.commit();
    }

    private static int maxVersion(Connection c) throws SQLException {
        try (Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT COALESCE(MAX(version),0) FROM wf_schema_version")) {
            rs.next();
            return rs.getInt(1);
        }
    }

    private static boolean tableExists(Connection c, String t) throws SQLException {
        try (ResultSet rs = c.getMetaData().getTables(null, null, t.toUpperCase(), new String[]{"TABLE"})) {
            return rs.next();
        }
    }

    @Test @DisplayName("APPLY runs pending migrations, records versions + checksums, and is idempotent")
    void applyAndReRun() throws Exception {
        List<Migration> migs = List.of(
                m(1, "CREATE TABLE IF NOT EXISTS t1 (id INT)"),
                m(2, "CREATE TABLE IF NOT EXISTS t2 (id INT)"));
        try (Connection c = h2()) {
            run(c, migs, MigrationMode.APPLY);
            assertEquals(2, maxVersion(c));
            assertTrue(tableExists(c, "t1") && tableExists(c, "t2"));
            // a second APPLY is a clean no-op (checksums match, nothing pending)
            assertDoesNotThrow(() -> run(c, migs, MigrationMode.APPLY));
            assertEquals(2, maxVersion(c));
        }
    }

    @Test @DisplayName("editing a released migration is caught as drift")
    void drift() throws Exception {
        try (Connection c = h2()) {
            run(c, List.of(m(1, "CREATE TABLE IF NOT EXISTS t1 (id INT)")), MigrationMode.APPLY);
            List<Migration> edited = List.of(m(1, "CREATE TABLE IF NOT EXISTS t1 (id BIGINT)"));
            SQLException e = assertThrows(SQLException.class, () -> run(c, edited, MigrationMode.APPLY));
            assertTrue(e.getMessage().contains("drift"), e.getMessage());
        }
    }

    @Test @DisplayName("VERIFY applies nothing: it fails when behind and passes when current")
    void verifyMode() throws Exception {
        List<Migration> migs = List.of(
                m(1, "CREATE TABLE IF NOT EXISTS t1 (id INT)"),
                m(2, "CREATE TABLE IF NOT EXISTS t2 (id INT)"));
        try (Connection c = h2()) {
            run(c, List.of(migs.get(0)), MigrationMode.APPLY);   // only V1 applied

            SQLException e = assertThrows(SQLException.class, () -> run(c, migs, MigrationMode.VERIFY));
            assertTrue(e.getMessage().contains("behind"), e.getMessage());
            assertFalse(tableExists(c, "t2"), "VERIFY must not create the pending table");

            run(c, migs, MigrationMode.APPLY);                   // now bring it current
            assertDoesNotThrow(() -> run(c, migs, MigrationMode.VERIFY));
        }
    }

    @Test @DisplayName("a legacy version row (no checksum) is backfilled on APPLY, then drift-protected")
    void legacyBackfill() throws Exception {
        try (Connection c = h2()) {
            try (Statement s = c.createStatement()) {   // simulate a pre-checksum schema_version table
                s.execute("CREATE TABLE wf_schema_version (version INT PRIMARY KEY, name VARCHAR(200) NOT NULL, " +
                        "applied_at BIGINT NOT NULL)");
                s.execute("INSERT INTO wf_schema_version (version, name, applied_at) VALUES (1, 'v1', 0)");
                s.execute("CREATE TABLE t1 (id INT)");
            }
            c.commit();

            List<Migration> migs = List.of(m(1, "CREATE TABLE IF NOT EXISTS t1 (id INT)"));
            run(c, migs, MigrationMode.APPLY);   // adds the checksum column + backfills V1
            assertDoesNotThrow(() -> run(c, migs, MigrationMode.VERIFY), "backfilled row now verifies");

            // Proof the backfill used the code's checksum: editing V1 is now caught as drift.
            List<Migration> edited = List.of(m(1, "CREATE TABLE IF NOT EXISTS t1 (id BIGINT)"));
            assertThrows(SQLException.class, () -> run(c, edited, MigrationMode.VERIFY));
        }
    }
}
