package com.wiggle.tests;

import com.wiggle.jdbc.Dialect;
import com.wiggle.jdbc.JdbcStorage;
import com.wiggle.postgres.H2Dialect;
import com.wiggle.postgres.PostgresDialect;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The dialect layer with no database attached: the SQL each dialect emits for DDL, row limiting,
 * conflict handling and the schedule upsert.
 *
 * <p>Two dialects remain. PostgreSQL is the deployment target and the reference SQL. H2 (in
 * PostgreSQL mode) exists so the suite can run the JDBC store, its migrations and the generic claim
 * path with nothing installed -- it is not a deployment target, and the difference that matters is
 * that it cannot do {@code SKIP LOCKED}, so it claims by compare-and-set instead.
 */
class DialectTest {

    @Test @DisplayName("PostgreSQL is the reference: single-statement claim, ON CONFLICT ignore")
    void postgres() {
        PostgresDialect d = new PostgresDialect();
        assertTrue(d.supportsSkipLocked());
        assertTrue(d.supportsReturning());
        assertEquals("INSERT INTO t VALUES (?) ON CONFLICT DO NOTHING", d.insertIgnore("INSERT INTO t VALUES (?)", "c"));
        assertTrue(d.scheduleUpsert().contains("ON CONFLICT (id) DO UPDATE"));
    }

    @Test @DisplayName("H2 keeps ON CONFLICT but has no SKIP LOCKED, so it claims via compare-and-set")
    void h2() {
        H2Dialect d = new H2Dialect();
        assertFalse(d.supportsSkipLocked());
        assertFalse(d.supportsReturning());
        assertEquals("FETCH FIRST 1 ROWS ONLY", d.firstRow());
    }

    @Test @DisplayName("the two dialects differ only in the claim primitives and the migration lock")
    void theyAgreeOnEverythingElse() {
        PostgresDialect pg = new PostgresDialect();
        H2Dialect h2 = new H2Dialect();
        // Same schema, same upserts: H2 in PostgreSQL mode takes the store's SQL verbatim, which is
        // why there is no statement-rewriting hook on Dialect any more.
        assertEquals(pg.scheduleUpsert(), h2.scheduleUpsert(), "the schedule upsert is identical");
        assertEquals(pg.insertIgnore("INSERT INTO t VALUES (?)", "c"),
                h2.insertIgnore("INSERT INTO t VALUES (?)", "c"), "conflict handling is identical");
        // And the difference that does matter.
        assertTrue(pg.supportsSkipLocked() && pg.supportsReturning(), "PostgreSQL claims in one statement");
        assertFalse(h2.supportsSkipLocked() || h2.supportsReturning(), "H2 falls back to compare-and-set");
    }
}
