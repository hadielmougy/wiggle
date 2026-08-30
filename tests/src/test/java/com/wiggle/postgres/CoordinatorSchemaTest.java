package com.wiggle.postgres;

import com.wiggle.jdbc.JdbcStorage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The cell and coordinator schemas live on separate baseline lineages in the same JDBC store:
 * {@code migrate()} applies the cell tables, {@code coordinatorStore()} applies the {@code coord_*}
 * tables. Each refuses a database initialised for the other (baseline guard).
 */
class CoordinatorSchemaTest {

    private static String h2(String tag) {
        return "jdbc:h2:mem:" + tag + "-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
    }

    private static void query(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            rs.next();
        }
    }

    private static boolean chainContains(Throwable t, String needle) {
        for (Throwable e = t; e != null; e = e.getCause()) {
            if (e.getMessage() != null && e.getMessage().contains(needle)) return true;
        }
        return false;
    }

    @Test @DisplayName("coordinatorStore() creates coord_* + coord_leader, and no engine tables")
    void coordinatorSchema() throws Exception {
        String url = h2("coord");
        try (JdbcStorage s = new JdbcStorage(url, "sa", "", 2, new H2Dialect())) {
            s.coordinatorStore();   // migrates the coord schema
            try (Connection c = DriverManager.getConnection(url, "sa", "")) {
                assertDoesNotThrow(() -> query(c, "SELECT * FROM coord_policy"));
                assertDoesNotThrow(() -> query(c, "SELECT * FROM coord_node"));
                assertDoesNotThrow(() -> query(c, "SELECT * FROM coord_definition"));
                assertDoesNotThrow(() -> query(c, "SELECT * FROM coord_leader"), "coordinator election lease");
                assertThrows(SQLException.class, () -> query(c, "SELECT * FROM wf_token"),
                        "coordinator has no engine tables");
            }
        }
    }

    @Test @DisplayName("migrate() creates the engine tables and no coord_* tables")
    void cellSchema() throws Exception {
        String url = h2("cell");
        try (JdbcStorage s = new JdbcStorage(url, "sa", "", 2, new H2Dialect())) {
            s.migrate();
            try (Connection c = DriverManager.getConnection(url, "sa", "")) {
                assertDoesNotThrow(() -> query(c, "SELECT * FROM wf_token"));
                assertThrows(SQLException.class, () -> query(c, "SELECT * FROM coord_policy"),
                        "cell has no coordinator tables");
            }
        }
    }

    @Test @DisplayName("the coordinator schema refuses a database initialised as a cell")
    void coordinatorOnCellDbRejected() throws Exception {
        String url = h2("mix1");
        try (JdbcStorage s = new JdbcStorage(url, "sa", "", 2, new H2Dialect())) {
            s.migrate();   // cell baseline
            RuntimeException ex = assertThrows(RuntimeException.class, s::coordinatorStore);
            assertTrue(chainContains(ex, "baseline mismatch"), () -> "unexpected: " + ex);
        }
    }

    @Test @DisplayName("the cell schema refuses a database initialised as a coordinator")
    void cellOnCoordinatorDbRejected() throws Exception {
        String url = h2("mix2");
        try (JdbcStorage s = new JdbcStorage(url, "sa", "", 2, new H2Dialect())) {
            s.coordinatorStore();   // coordinator baseline
            RuntimeException ex = assertThrows(RuntimeException.class, s::migrate);
            assertTrue(chainContains(ex, "baseline mismatch"), () -> "unexpected: " + ex);
        }
    }
}
