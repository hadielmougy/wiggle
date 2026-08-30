package com.wiggle.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * The per-database differences the JDBC store has to bend around, factored out of
 * {@link JdbcStorage} so a single store body serves PostgreSQL, H2, MySQL and Oracle.
 *
 * <p>The store writes canonical, PostgreSQL-flavoured SQL; a {@code Dialect} rewrites the
 * handful of fragments that are not portable -- DDL type names and {@code IF NOT EXISTS}
 * support, row limiting, conflict/upsert syntax, and the concurrency primitives used by the
 * task-claim path. Each database module ({@code wiggle-postgres}, {@code wiggle-mysql},
 * {@code wiggle-oracle}) supplies one implementation and detects it from the JDBC URL.
 */
public interface Dialect {

    /** Short identifier, e.g. {@code "postgresql"}, {@code "h2"}, {@code "mysql"}, {@code "oracle"}. */
    String id();

    /**
     * Rewrites one canonical (PostgreSQL-flavoured) DDL statement into this dialect's SQL --
     * mapping types ({@code TEXT}, {@code BIGINT}) and stripping {@code IF NOT EXISTS} clauses the
     * dialect does not accept. The default is identity (PostgreSQL/H2 take the canonical DDL as-is).
     */
    default String ddl(String canonicalSql) { return canonicalSql; }

    /**
     * Rewrites a {@code LIMIT ?} (or {@code LIMIT n}) row-limiting clause into the dialect's form,
     * preserving the {@code ?} parameter position. Default is identity (PostgreSQL/H2/MySQL all
     * accept {@code LIMIT}); Oracle rewrites it to {@code FETCH FIRST ? ROWS ONLY}.
     */
    default String limit(String sql) { return sql; }

    /** The single-row limiter for existence probes: {@code "LIMIT 1"} or {@code "FETCH FIRST 1 ROWS ONLY"}. */
    default String firstRow() { return "LIMIT 1"; }

    /**
     * A locking table hint placed <em>after</em> the table name for a pessimistic row read
     * ({@code lockInstance}). Empty for dialects that instead express it as a trailing
     * {@link #forUpdateSuffix()} clause; SQL Server uses {@code WITH (UPDLOCK, ROWLOCK)} here.
     */
    default String forUpdateHint() { return ""; }

    /**
     * The trailing clause for a pessimistic row read ({@code lockInstance}). {@code FOR UPDATE} on
     * PostgreSQL/H2/MySQL/Oracle; empty on SQL Server, which locks via {@link #forUpdateHint()}.
     */
    default String forUpdateSuffix() { return "FOR UPDATE"; }

    /** Whether {@code SELECT ... FOR UPDATE SKIP LOCKED} can drive the task claim. */
    default boolean supportsSkipLocked() { return false; }

    /** Whether {@code UPDATE ... RETURNING} is available (PostgreSQL), letting the claim be one statement. */
    default boolean supportsReturning() { return false; }

    /**
     * Wraps an {@code INSERT} so that a primary-key collision is silently ignored (idempotent
     * re-registration). {@code noopColumn} is a non-key column self-assigned by dialects that
     * express this as an upsert (MySQL's {@code ON DUPLICATE KEY UPDATE}). Dialects with no inline
     * form (Oracle) return the statement unchanged and rely on {@link #isDuplicateKey}.
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
     * advisory lock; other dialects have no cheap equivalent and rely on run-once version tracking,
     * so the default is a no-op.
     */
    default void acquireMigrationLock(Connection c) throws SQLException { }

    /**
     * Whether a DDL error during migration is a benign "object/column already exists" and may be
     * ignored -- the idempotency that {@code IF NOT EXISTS} gives on PostgreSQL/H2/MySQL. Oracle has
     * no {@code IF NOT EXISTS} on older versions <em>and</em> auto-commits each DDL statement, so a
     * restart or a partially-applied migration can re-encounter an existing object; tolerating that
     * here keeps migration idempotent. Default is strict (nothing tolerated).
     */
    default boolean isBenignMigrationError(SQLException e) { return false; }
}
