package com.wiggle.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * The per-database differences the JDBC store has to bend around, factored out of
 * {@link JdbcStorage}.
 *
 * <p>Two implementations, both in {@code wiggle-postgres}. PostgreSQL is the deployment target and
 * the SQL the store writes; H2 (in PostgreSQL mode) is for tests and local runs. H2 takes that SQL
 * verbatim -- the same DDL, the same {@code ON CONFLICT} upserts, the same {@code LIMIT} -- so there
 * is no statement rewriting here at all. What is left are the few things the store has to ask
 * rather than assume: whether {@code SKIP LOCKED} and {@code RETURNING} are available (they are not
 * on H2, which claims by compare-and-set instead of in one statement), how to take the migration
 * lock, and how a duplicate key and a momentary failure surface.
 */
public interface Dialect {

    /** Short identifier: {@code "postgresql"} or {@code "h2"}. */
    String id();

    /** Whether {@code SELECT ... FOR UPDATE SKIP LOCKED} can drive the task claim. */
    default boolean supportsSkipLocked() { return false; }

    /** Whether {@code UPDATE ... RETURNING} is available (PostgreSQL), letting the claim be one statement. */
    default boolean supportsReturning() { return false; }

    /**
     * Wraps an {@code INSERT} so that a primary-key collision is silently ignored (idempotent
     * re-registration). Both dialects take PostgreSQL's {@code ON CONFLICT DO NOTHING}, so this is
     * not a per-dialect choice; a backend with no inline form would override it to return the
     * statement unchanged and lean on {@link #isDuplicateKey} instead.
     */
    default String insertIgnore(String insertSql) {
        return insertSql + " ON CONFLICT DO NOTHING";
    }

    /** Whether the exception (or any in its chain) is a duplicate/unique-key violation. */
    default boolean isDuplicateKey(SQLException e) {
        for (SQLException cur = e; cur != null; cur = cur.getNextException()) {
            if (cur instanceof java.sql.SQLIntegrityConstraintViolationException) return true;
            if ("23505".equals(cur.getSQLState())) return true;   // pgjdbc raises a plain PSQLException
        }
        return false;
    }

    /**
     * Whether the exception (or any in its chain) is a <em>momentary</em> inability to serve the
     * statement -- one where the transaction is gone and the same work would very likely succeed on
     * a fresh connection. A connection that dropped, a pool that timed out, a deadlock victim, a
     * serialization failure, a server that is restarting.
     *
     * <p>This decides whether {@code JdbcStorage} re-runs a rolled-back transaction, so it MUST be
     * conservative: anything the caller could mistake for "nothing was applied" when work in fact
     * landed does not belong here. A failed <em>commit</em> is never transient -- its outcome is
     * unknown, not undone -- and the store classifies that case itself rather than asking.
     *
     * <p>The default is the PostgreSQL answer, which is this interface's reference dialect: the JDBC
     * transient/recoverable exception types where a driver bothers to throw them (HikariCP's pool
     * timeout, H2's lock timeout), plus the SQL states that pgjdbc reports instead, since it raises a
     * plain {@code PSQLException} for everything.
     */
    default boolean isTransient(SQLException e) {
        for (SQLException cur = e; cur != null; cur = cur.getNextException()) {
            if (cur instanceof java.sql.SQLTransientException
                    || cur instanceof java.sql.SQLRecoverableException) return true;
            String state = cur.getSQLState();
            if (state == null) continue;
            if (state.startsWith("08")) return true;            // connection exception
            switch (state) {
                case "40001",                                   // serialization failure
                     "40P01",                                   // deadlock detected
                     "53300",                                   // too many connections
                     "55P03",                                   // lock not available
                     "57P01",                                   // admin shutdown
                     "57P02",                                   // crash shutdown
                     "57P03",                                   // cannot connect now (recovering)
                     "HYT00" -> { return true; }                 // lock timeout (H2, ODBC-style)
                default -> { }
            }
        }
        return false;
    }

    /**
     * The full upsert for {@code wf_schedule} keyed by {@code id}, binding seven parameters in
     * insert-column order: id, workflow, interval_millis, cron, context, next_fire_at, created_at.
     * Both dialects take this verbatim; a backend that spells its upsert differently (a {@code MERGE})
     * would override it, keeping the same parameter order.
     */
    default String scheduleUpsert() {
        return "INSERT INTO wf_schedule (id,workflow,interval_millis,cron,context,next_fire_at,created_at) " +
                "VALUES (?,?,?,?,?,?,?) ON CONFLICT (id) DO UPDATE SET workflow=EXCLUDED.workflow, " +
                "interval_millis=EXCLUDED.interval_millis, cron=EXCLUDED.cron, " +
                "context=EXCLUDED.context, next_fire_at=EXCLUDED.next_fire_at";
    }

    /**
     * Acquires a transaction-scoped lock serialising migrations across nodes. PostgreSQL uses an
     * advisory lock; H2 has no cheap equivalent and relies on run-once version tracking, so the
     * default is a no-op.
     */
    default void acquireMigrationLock(Connection c) throws SQLException { }

}
