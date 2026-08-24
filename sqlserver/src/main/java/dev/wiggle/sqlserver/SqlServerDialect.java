package dev.wiggle.sqlserver;

import dev.wiggle.jdbc.Dialect;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Microsoft SQL Server (T-SQL). It diverges from the canonical PostgreSQL SQL in several places:
 *
 * <ul>
 *   <li><b>Row limiting</b> — no {@code LIMIT}; {@code LIMIT ?} becomes
 *       {@code OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY} (which is why every limited query carries an
 *       {@code ORDER BY}).</li>
 *   <li><b>Pessimistic read</b> — no {@code FOR UPDATE}; {@code lockInstance} locks with the
 *       {@code WITH (UPDLOCK, ROWLOCK)} table hint instead.</li>
 *   <li><b>Task claim</b> — the portable compare-and-set path (SQL Server's queue-friendly
 *       {@code READPAST} hint is a possible future optimisation).</li>
 *   <li><b>DDL</b> — no {@code IF NOT EXISTS} and no {@code COLUMN} keyword on {@code ALTER TABLE
 *       ADD}; {@code TEXT} is deprecated, so it maps to {@code VARCHAR(MAX)}. "Already exists"
 *       errors during migration are tolerated for idempotent restarts.</li>
 *   <li><b>Upserts</b> — no inline conflict-ignore (a duplicate key is swallowed); the schedule
 *       upsert is a {@code MERGE}. Migrations are serialised across nodes with a transaction-scoped
 *       {@code sp_getapplock}.</li>
 * </ul>
 */
public final class SqlServerDialect implements Dialect {

    @Override public String id() { return "sqlserver"; }

    @Override public String limit(String sql) {
        return sql.replace("LIMIT ?", "OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY");
    }

    // Used only by the graph-existence probe (a query over wf_graph_node), so ORDER BY node_id is
    // valid; SQL Server's FETCH requires an ORDER BY.
    @Override public String firstRow() { return "ORDER BY node_id OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY"; }

    @Override public String forUpdateHint() { return "WITH (UPDLOCK, ROWLOCK)"; }

    @Override public String forUpdateSuffix() { return ""; }

    @Override public String ddl(String canonicalSql) {
        String s = canonicalSql.replaceAll("(?i)\\s+IF NOT EXISTS", "");
        // SQL Server's ALTER TABLE has no COLUMN keyword: it's `ADD col type`.
        s = s.replaceAll("(?i)\\bADD COLUMN\\b", "ADD");
        // TEXT is deprecated in SQL Server; VARCHAR(MAX) is the supported large-text type.
        s = s.replaceAll("\\bTEXT\\b", "VARCHAR(MAX)");
        return s;
    }

    @Override public boolean isBenignMigrationError(SQLException e) {
        for (SQLException cur = e; cur != null; cur = cur.getNextException()) {
            int code = cur.getErrorCode();
            if (code == 2714    // There is already an object named '...'
                    || code == 1913    // The operation failed because an index already exists
                    || code == 2705) { // Column names in each table must be unique
                return true;
            }
        }
        return false;
    }

    @Override public boolean isDuplicateKey(SQLException e) {
        for (SQLException cur = e; cur != null; cur = cur.getNextException()) {
            int code = cur.getErrorCode();
            if (code == 2627 || code == 2601) return true;  // PK violation / unique index violation
        }
        return false;
    }

    @Override public String insertIgnore(String insertSql, String noopColumn) {
        return insertSql;  // no inline ignore; a real duplicate is caught via isDuplicateKey
    }

    @Override public String scheduleUpsert() {
        return "MERGE wf_schedule AS d USING (VALUES (?,?,?,?,?,?,?)) " +
                "AS s (id,workflow,interval_millis,cron,context,next_fire_at,created_at) ON d.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET workflow=s.workflow, interval_millis=s.interval_millis, " +
                "cron=s.cron, context=s.context, next_fire_at=s.next_fire_at " +
                "WHEN NOT MATCHED THEN INSERT (id,workflow,interval_millis,cron,context,next_fire_at,created_at) " +
                "VALUES (s.id,s.workflow,s.interval_millis,s.cron,s.context,s.next_fire_at,s.created_at);";
    }

    @Override public void acquireMigrationLock(Connection c) throws SQLException {
        // Transaction-scoped application lock (released on commit/rollback), serialising migrations
        // across nodes exactly as PostgreSQL's advisory lock does.
        try (Statement st = c.createStatement()) {
            st.execute("EXEC sp_getapplock @Resource = 'wiggle_migrate', " +
                    "@LockMode = 'Exclusive', @LockOwner = 'Transaction'");
        }
    }
}
