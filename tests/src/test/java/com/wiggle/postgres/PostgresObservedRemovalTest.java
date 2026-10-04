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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/** Migration 26 on PostgreSQL, from a schema that still has OBSERVED execution's tables. Opt-in with
 *  {@code WIGGLE_TEST_PG_URL}; the user must be allowed to create databases. */
@EnabledIfEnvironmentVariable(named = "WIGGLE_TEST_PG_URL", matches = ".+")
class PostgresObservedRemovalTest {

    @Test @DisplayName("migration 26 cancels an open observed run and drops OBSERVED's schema on PostgreSQL")
    void migrationOnPostgres() throws Exception {
        String db = "wiggle_obs_" + Long.toHexString(System.nanoTime());
        String base = TestDb.url("PG");
        String url = base.substring(0, base.lastIndexOf('/') + 1) + db;
        try (Connection admin = DriverManager.getConnection(base, TestDb.user("PG"), TestDb.password("PG"));
             Statement s = admin.createStatement()) {
            s.execute("CREATE DATABASE " + db);
        }
        try {
            try (Connection c = DriverManager.getConnection(url, TestDb.user("PG"), TestDb.password("PG"))) {
                c.setAutoCommit(false);
                JdbcStorage.runMigrations(c, JdbcStorage.MIGRATIONS.subList(0, 25), new PostgresDialect());
                c.commit();
                try (Statement s = c.createStatement()) {
                    s.execute("INSERT INTO wf_instance (id,workflow,version,status,context,created_at,updated_at,"
                            + "revision,settle_at) VALUES ('wfo_open','w',1,'RUNNING','{}',1,1,0,99)");
                }
                c.commit();
                JdbcStorage.runMigrations(c, JdbcStorage.MIGRATIONS, new PostgresDialect());
                c.commit();
                try (Statement s = c.createStatement()) {
                    try (ResultSet rs = s.executeQuery("SELECT status FROM wf_instance WHERE id='wfo_open'")) {
                        rs.next();
                        assertEquals("CANCELLED", rs.getString(1));
                    }
                    try (ResultSet rs = c.getMetaData().getColumns(null, null, "wf_instance", "settle_at")) {
                        assertFalse(rs.next(), "settle_at is gone");
                    }
                    try (ResultSet rs = c.getMetaData().getTables(null, null, "wf_anomaly", null)) {
                        assertFalse(rs.next(), "wf_anomaly is gone");
                    }
                }
            }
        } finally {
            try (Connection admin = DriverManager.getConnection(base, TestDb.user("PG"), TestDb.password("PG"));
                 Statement s = admin.createStatement()) {
                s.execute("DROP DATABASE IF EXISTS " + db + " WITH (FORCE)");
            }
        }
    }
}
