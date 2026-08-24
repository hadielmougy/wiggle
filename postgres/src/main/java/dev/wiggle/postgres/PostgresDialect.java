package dev.wiggle.postgres;

import dev.wiggle.jdbc.Dialect;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * PostgreSQL: the reference dialect. Canonical SQL is written for it, so most methods are the
 * identity default. It is the only dialect that can claim tasks in a single statement
 * ({@code FOR UPDATE SKIP LOCKED} + {@code UPDATE ... RETURNING}) and the only one with a cheap
 * cross-node migration lock (a transaction-scoped advisory lock).
 */
public final class PostgresDialect implements Dialect {

    @Override public String id() { return "postgresql"; }

    @Override public boolean supportsSkipLocked() { return true; }

    @Override public boolean supportsReturning() { return true; }

    @Override public String insertIgnore(String insertSql, String noopColumn) {
        return insertSql + " ON CONFLICT DO NOTHING";
    }

    @Override public String scheduleUpsert() {
        return "INSERT INTO wf_schedule (id,workflow,interval_millis,cron,context,next_fire_at,created_at) " +
                "VALUES (?,?,?,?,?,?,?) ON CONFLICT (id) DO UPDATE SET workflow=EXCLUDED.workflow, " +
                "interval_millis=EXCLUDED.interval_millis, cron=EXCLUDED.cron, " +
                "context=EXCLUDED.context, next_fire_at=EXCLUDED.next_fire_at";
    }

    @Override public void acquireMigrationLock(Connection c) throws SQLException {
        try (Statement st = c.createStatement()) {
            st.execute("SELECT pg_advisory_xact_lock(7420398115703004)");
        }
    }
}
