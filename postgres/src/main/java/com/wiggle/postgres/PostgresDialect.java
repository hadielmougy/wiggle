package com.wiggle.postgres;

import com.wiggle.jdbc.Dialect;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * PostgreSQL: the deployment target, and the SQL the store is written in -- so the defaults on
 * {@link Dialect} are already its dialect and all it declares is what it can do that H2 cannot.
 * It claims tasks in a single statement ({@code FOR UPDATE SKIP LOCKED} + {@code UPDATE ...
 * RETURNING}) and has a cheap cross-node migration lock (a transaction-scoped advisory lock).
 */
public final class PostgresDialect implements Dialect {

    @Override public String id() { return "postgresql"; }

    @Override public boolean supportsSkipLocked() { return true; }

    @Override public boolean supportsReturning() { return true; }

    @Override public void acquireMigrationLock(Connection c) throws SQLException {
        try (Statement st = c.createStatement()) {
            st.execute("SELECT pg_advisory_xact_lock(7420398115703004)");
        }
    }
}
