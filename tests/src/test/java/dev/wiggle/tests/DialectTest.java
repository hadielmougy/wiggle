package dev.wiggle.tests;

import dev.wiggle.jdbc.Dialect;
import dev.wiggle.postgres.H2Dialect;
import dev.wiggle.jdbc.JdbcStorage;
import dev.wiggle.postgres.PostgresDialect;
import dev.wiggle.mysql.MySqlDialect;
import dev.wiggle.oracle.OracleDialect;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The dialect layer with no database attached: the SQL each dialect emits for DDL, row limiting,
 * conflict handling and the schedule upsert. This is the only coverage MySQL and Oracle get in CI
 * (their integration tests are opt-in against a real server), so it asserts the fragments that make
 * the canonical PostgreSQL SQL portable.
 */
class DialectTest {

    @Test @DisplayName("PostgreSQL is the reference: single-statement claim, ON CONFLICT ignore")
    void postgres() {
        PostgresDialect d = new PostgresDialect();
        assertTrue(d.supportsSkipLocked());
        assertTrue(d.supportsReturning());
        assertEquals("INSERT INTO t VALUES (?) ON CONFLICT DO NOTHING", d.insertIgnore("INSERT INTO t VALUES (?)", "c"));
        assertTrue(d.scheduleUpsert().contains("ON CONFLICT (id) DO UPDATE"));
        // Canonical SQL passes through untouched.
        assertEquals("CREATE TABLE IF NOT EXISTS x (a BIGINT)", d.ddl("CREATE TABLE IF NOT EXISTS x (a BIGINT)"));
    }

    @Test @DisplayName("H2 keeps ON CONFLICT but has no SKIP LOCKED, so it claims via compare-and-set")
    void h2() {
        H2Dialect d = new H2Dialect();
        assertFalse(d.supportsSkipLocked());
        assertFalse(d.supportsReturning());
        assertEquals("FETCH FIRST 1 ROWS ONLY", d.firstRow());
    }

    @Test @DisplayName("MySQL: SKIP LOCKED without RETURNING, ON DUPLICATE KEY upsert, IF-NOT-EXISTS stripped off indexes")
    void mysql() {
        MySqlDialect d = new MySqlDialect();
        assertTrue(d.supportsSkipLocked());
        assertFalse(d.supportsReturning());
        assertEquals("INSERT INTO t VALUES (?) ON DUPLICATE KEY UPDATE kind=kind",
                d.insertIgnore("INSERT INTO t VALUES (?)", "kind"));
        assertTrue(d.scheduleUpsert().contains("ON DUPLICATE KEY UPDATE"));
        // Tables keep IF NOT EXISTS; indexes and add-column drop it (MySQL rejects them there).
        assertEquals("CREATE TABLE IF NOT EXISTS x (a INT)", d.ddl("CREATE TABLE IF NOT EXISTS x (a INT)"));
        assertEquals("CREATE INDEX ix ON x (a)", d.ddl("CREATE INDEX IF NOT EXISTS ix ON x (a)"));
        assertEquals("CREATE UNIQUE INDEX ix ON x (a)", d.ddl("CREATE UNIQUE INDEX IF NOT EXISTS ix ON x (a)"));
        assertEquals("ALTER TABLE x ADD COLUMN c INT", d.ddl("ALTER TABLE x ADD COLUMN IF NOT EXISTS c INT"));
    }

    @Test @DisplayName("Oracle: no IF NOT EXISTS, TEXT->CLOB, BIGINT->NUMBER, LIMIT->FETCH, nullable join_stack, MERGE upsert")
    void oracle() {
        OracleDialect d = new OracleDialect();
        assertFalse(d.supportsSkipLocked(), "FETCH + FOR UPDATE is illegal on Oracle, so it uses compare-and-set");
        assertEquals("FETCH FIRST 1 ROWS ONLY", d.firstRow());

        // Row limiting keeps the parameter, only the syntax changes.
        assertEquals("SELECT * FROM t ORDER BY a FETCH FIRST ? ROWS ONLY",
                d.limit("SELECT * FROM t ORDER BY a LIMIT ?"));

        // DDL rewrites: no IF NOT EXISTS, portable type names.
        String ddl = d.ddl("CREATE TABLE IF NOT EXISTS x (a BIGINT NOT NULL, b TEXT)");
        assertFalse(ddl.contains("IF NOT EXISTS"), ddl);
        assertTrue(ddl.contains("NUMBER(19)"), ddl);
        assertTrue(ddl.contains("CLOB"), ddl);
        assertFalse(ddl.contains("BIGINT"), ddl);
        assertFalse(ddl.contains("TEXT"), ddl);

        // The empty-string sentinel column loses its NOT NULL (Oracle stores '' as NULL).
        assertEquals("join_stack VARCHAR(1000)",
                d.ddl("join_stack     VARCHAR(1000) NOT NULL"));

        // Oracle's ALTER TABLE has no COLUMN keyword.
        assertEquals("ALTER TABLE x ADD c NUMBER(19)",
                d.ddl("ALTER TABLE x ADD COLUMN IF NOT EXISTS c BIGINT"));

        // "already exists" during migration is benign (no IF NOT EXISTS + auto-commit DDL).
        assertTrue(d.isBenignMigrationError(new SQLException("name is already used", "42000", 955)));
        assertTrue(d.isBenignMigrationError(new SQLException("column already exists", "42000", 1430)));
        assertFalse(d.isBenignMigrationError(new SQLException("syntax error", "42000", 900)));

        assertTrue(d.scheduleUpsert().startsWith("MERGE INTO wf_schedule"));
        assertTrue(d.scheduleUpsert().contains("FROM dual"));

        // ORA-00001 (unique constraint violated) is recognised as a duplicate key.
        assertTrue(d.isDuplicateKey(new SQLException("unique constraint violated", "23000", 1)));
        assertFalse(d.isDuplicateKey(new SQLException("some other error", "42000", 942)));
    }

    @Test @DisplayName("the whole baseline schema renders for every dialect without leaving a portability hazard")
    void baselineRendersEverywhere() {
        for (Dialect d : new Dialect[]{new PostgresDialect(), new H2Dialect(), new MySqlDialect(), new OracleDialect()}) {
            for (JdbcStorage.Migration m : JdbcStorage.MIGRATIONS) {
                for (String stmt : m.sql().split(";")) {
                    if (stmt.isBlank()) continue;
                    String rendered = d.ddl(stmt);
                    if (d instanceof OracleDialect) {
                        assertFalse(rendered.contains("IF NOT EXISTS"), "oracle: " + rendered);
                        assertFalse(rendered.contains("ADD COLUMN"), "oracle keeps ADD COLUMN: " + rendered);
                        assertFalse(rendered.matches("(?s).*\\bBIGINT\\b.*"), "oracle keeps BIGINT: " + rendered);
                        assertFalse(rendered.matches("(?s).*\\bTEXT\\b.*"), "oracle keeps TEXT: " + rendered);
                    }
                    if (d instanceof MySqlDialect) {
                        assertFalse(rendered.contains("INDEX IF NOT EXISTS"), "mysql index: " + rendered);
                        assertFalse(rendered.contains("ADD COLUMN IF NOT EXISTS"), "mysql add column: " + rendered);
                    }
                }
            }
        }
    }
}
