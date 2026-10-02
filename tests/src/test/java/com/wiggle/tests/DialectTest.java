package com.wiggle.tests;

import com.wiggle.jdbc.Dialect;
import com.wiggle.jdbc.JdbcStorage;
import com.wiggle.postgres.H2Dialect;
import com.wiggle.postgres.PostgresDialect;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The dialect layer with no database attached: the SQL each dialect emits for DDL, row limiting,
 * conflict handling and the schedule upsert, plus the two failures it has to recognise -- a duplicate
 * key, and a momentary failure the store may replay.
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
        assertEquals("INSERT INTO t VALUES (?) ON CONFLICT DO NOTHING", d.insertIgnore("INSERT INTO t VALUES (?)"));
        assertTrue(d.scheduleUpsert().contains("ON CONFLICT (id) DO UPDATE"));
    }

    @Test @DisplayName("H2 keeps ON CONFLICT DO NOTHING, merges instead of upserting, and has no SKIP LOCKED")
    void h2() {
        H2Dialect d = new H2Dialect();
        assertFalse(d.supportsSkipLocked());
        assertFalse(d.supportsReturning());
        assertEquals("INSERT INTO t VALUES (?) ON CONFLICT DO NOTHING", d.insertIgnore("INSERT INTO t VALUES (?)"));
        assertTrue(d.scheduleUpsert().contains("MERGE INTO wf_schedule"),
                "ON CONFLICT stops at DO NOTHING here, so an upsert that updates is a MERGE");
    }

    @Test @DisplayName("a momentary failure is recognised from the SQL state, whichever type the driver threw")
    void transientStates() {
        for (Dialect d : new Dialect[] { new PostgresDialect(), new H2Dialect() }) {
            // pgjdbc raises a plain PSQLException for everything, so the state is all there is to go on.
            assertTrue(d.isTransient(new SQLException("connection closed", "08006")), d.id() + ": connection lost");
            assertTrue(d.isTransient(new SQLException("could not serialize", "40001")), d.id() + ": serialization");
            assertTrue(d.isTransient(new SQLException("deadlock detected", "40P01")), d.id() + ": deadlock victim");
            assertTrue(d.isTransient(new SQLException("too many clients", "53300")), d.id() + ": out of connections");
            assertTrue(d.isTransient(new SQLException("terminating connection", "57P01")), d.id() + ": shutdown");
            assertTrue(d.isTransient(new SQLException("lock timeout", "HYT00")), d.id() + ": lock timeout");

            // And the typed route, which is what HikariCP's pool timeout and H2's lock timeout throw.
            assertTrue(d.isTransient(new java.sql.SQLTransientConnectionException("pool timeout")),
                    d.id() + ": pool timeout");
            assertTrue(d.isTransient(new java.sql.SQLRecoverableException("socket closed")),
                    d.id() + ": recoverable");
        }
    }

    @Test @DisplayName("a refusal on the statement's own terms is not momentary, and neither is an unstated failure")
    void permanentStates() {
        PostgresDialect d = new PostgresDialect();
        assertFalse(d.isTransient(new SQLException("syntax error", "42601")), "a syntax error is final");
        assertFalse(d.isTransient(new SQLException("duplicate key", "23505")), "a constraint violation is final");
        assertFalse(d.isTransient(new SQLException("password authentication failed", "28P01")), "bad credentials");
        assertFalse(d.isTransient(new SQLException("no state at all")), "an unstated failure is not assumed retryable");
    }

    @Test @DisplayName("both predicates read the driver's whole getNextException chain")
    void chainedExceptions() {
        PostgresDialect d = new PostgresDialect();
        SQLException head = new SQLException("batch entry 0 failed", "42000");
        head.setNextException(new SQLException("deadlock detected", "40P01"));
        assertTrue(d.isTransient(head), "the transient one is further down the chain");

        SQLException dup = new SQLException("batch entry 0 failed", "42000");
        dup.setNextException(new SQLException("duplicate key value", "23505"));
        assertTrue(d.isDuplicateKey(dup), "pgjdbc reports 23505 without the typed subclass");
        assertFalse(d.isTransient(dup), "and a duplicate key is not momentary");
    }

    @Test
    @DisplayName("the two dialects differ in the claim primitives, the migration lock and the schedule upsert")
    void theyAgreeOnEverythingElse() {
        PostgresDialect pg = new PostgresDialect();
        H2Dialect h2 = new H2Dialect();
        // Same schema, and the same DO NOTHING conflict clause: that much H2 does take verbatim,
        // which is why insertIgnore is one shared default rather than two identical overrides.
        assertEquals(pg.insertIgnore("INSERT INTO t VALUES (?)"),
                h2.insertIgnore("INSERT INTO t VALUES (?)"), "conflict handling is identical");

        // The schedule upsert is not shared: H2 takes ON CONFLICT only as far as DO NOTHING, so it
        // spells this one as a MERGE. What has to agree is the shape the store binds to -- seven
        // positional parameters in insert-column order -- and not the text. That both statements
        // then behave the same is asserted by running them: server/store/StorageContract.
        assertTrue(pg.scheduleUpsert().contains("ON CONFLICT (id) DO UPDATE"), pg.scheduleUpsert());
        assertTrue(h2.scheduleUpsert().contains("MERGE INTO wf_schedule"), h2.scheduleUpsert());
        for (Dialect d : java.util.List.of(pg, h2)) {
            assertTrue(d.scheduleUpsert().contains("wf_schedule"), d.id() + " upserts wf_schedule");
            assertEquals(7, d.scheduleUpsert().chars().filter(c -> c == '?').count(),
                    d.id() + " binds the seven schedule columns, in insert-column order");
        }

        // And the difference that does matter.
        assertTrue(pg.supportsSkipLocked() && pg.supportsReturning(), "PostgreSQL claims in one statement");
        assertFalse(h2.supportsSkipLocked() || h2.supportsReturning(), "H2 falls back to compare-and-set");
    }
}
