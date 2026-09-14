package com.wiggle.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * The per-database differences the JDBC store has to bend around, factored out of
 * {@link JdbcStorage}.
 *
 * <p>Two implementations, both in {@code wiggle-postgres}. PostgreSQL is the deployment target and
 * the SQL the store writes; H2 (in PostgreSQL mode) is for tests and local runs. H2 takes that SQL
 * verbatim -- the same DDL, the same {@code ON CONFLICT} upserts -- so there is no statement
 * rewriting here at all. What is left are the few places the store has to ask rather than assume:
 * whether {@code SKIP LOCKED} and {@code RETURNING} are available (they are not on H2, which claims
 * by compare-and-set instead of in one statement), how to take the migration lock, and how a
 * duplicate key surfaces.
 */
public interface Dialect {

    /** Short identifier: {@code "postgresql"} or {@code "h2"}. */
    String id();

    /** The single-row limiter for existence probes: {@code "LIMIT 1"} or {@code "FETCH FIRST 1 ROWS ONLY"}. */
    default String firstRow() { return "LIMIT 1"; }

    /** Whether {@code SELECT ... FOR UPDATE SKIP LOCKED} can drive the task claim. */
    default boolean supportsSkipLocked() { return false; }

    /** Whether {@code UPDATE ... RETURNING} is available (PostgreSQL), letting the claim be one statement. */
    default boolean supportsReturning() { return false; }

    /**
     * Wraps an {@code INSERT} so that a primary-key collision is silently ignored (idempotent
     * re-registration). {@code noopColumn} is a non-key column self-assigned by dialects that
     * express this as an upsert (MySQL's {@code ON DUPLICATE KEY UPDATE}). Dialects with no inline
     * inline form would return the statement unchanged and rely on {@link #isDuplicateKey}.
     */
    String insertIgnore(String insertSql, String noopColumn);

    /** Whether the exception (or any in its chain) is a duplicate/unique-key violation. */
    default boolean isDuplicateKey(SQLException e) {
        for (SQLException cur = e; cur != null; cur = cur.getNextException()) {
            if (cur instanceof java.sql.SQLIntegrityConstraintViolationException) return true;
        }
        return false;
    }

    /**
     * The full upsert for {@code wf_schedule} keyed by {@code id}. Every dialect binds the same
     * seven parameters in insert-column order: id, workflow, interval_millis, cron, context,
     * next_fire_at, created_at -- so only the SQL text differs.
     */
    String scheduleUpsert();

    /**
     * Acquires a transaction-scoped lock serialising migrations across nodes. PostgreSQL uses an
     * advisory lock; H2 has no cheap equivalent and relies on run-once version tracking, so the
     * default is a no-op.
     */
    default void acquireMigrationLock(Connection c) throws SQLException { }

}
