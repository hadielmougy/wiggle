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

    @Override public boolean supportsPartialIndexes() { return true; }

    @Override public String codePointOrder(String column) { return column + " COLLATE \"C\""; }

    @Override public boolean supportsFullText() { return true; }

    @Override public boolean supportsJoinedLock() { return true; }

    /**
     * Prices a random page read at 1.1 sequential reads, as on SSD storage, so the claim walks the
     * ready index in claim order and stops at its last match rather than reading every ready row
     * to sort them when the planner under-estimates how many tasks a criteria predicate matches.
     * {@code wiggle.pg.randomPageCost} / {@code WIGGLE_PG_RANDOM_PAGE_COST} overrides the cost;
     * {@code server} keeps the server's own.
     */
    @Override public String connectionInitSql() {
        String v = System.getProperty("wiggle.pg.randomPageCost");
        if (v == null || v.isBlank()) v = System.getenv("WIGGLE_PG_RANDOM_PAGE_COST");
        if (v == null || v.isBlank()) v = "1.1";
        if (v.trim().equalsIgnoreCase("server")) return null;
        double cost;
        try {
            cost = Double.parseDouble(v.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("WIGGLE_PG_RANDOM_PAGE_COST is not a number or 'server': " + v);
        }
        if (!(cost > 0)) throw new IllegalArgumentException("WIGGLE_PG_RANDOM_PAGE_COST must be positive: " + v);
        return "SET random_page_cost = " + cost;
    }

    @Override public void acquireMigrationLock(Connection c) throws SQLException {
        try (Statement st = c.createStatement()) {
            st.execute("SELECT pg_advisory_xact_lock(7420398115703004)");
        }
    }
}
