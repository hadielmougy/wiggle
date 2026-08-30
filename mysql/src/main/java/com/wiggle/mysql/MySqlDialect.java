package com.wiggle.mysql;

import com.wiggle.jdbc.Dialect;

/**
 * MySQL / MariaDB (InnoDB). Row limiting uses the native {@code LIMIT}, and MySQL 8.0+ supports
 * {@code FOR UPDATE SKIP LOCKED}, so the task claim takes the two-step locked path (there is no
 * {@code RETURNING}). MySQL accepts {@code CREATE TABLE IF NOT EXISTS} but not the same clause on
 * {@code CREATE INDEX} or {@code ALTER TABLE ... ADD COLUMN}, so those are stripped -- safe because
 * migrations are version-tracked and run exactly once. Idempotent inserts use
 * {@code ON DUPLICATE KEY UPDATE}.
 */
public final class MySqlDialect implements Dialect {

    @Override public String id() { return "mysql"; }

    @Override public boolean supportsSkipLocked() { return true; }

    @Override public String ddl(String canonicalSql) {
        // Keep CREATE TABLE IF NOT EXISTS; drop the clause where MySQL rejects it.
        return canonicalSql
                .replace("INDEX IF NOT EXISTS", "INDEX")
                .replace("ADD COLUMN IF NOT EXISTS", "ADD COLUMN");
    }

    @Override public String insertIgnore(String insertSql, String noopColumn) {
        return insertSql + " ON DUPLICATE KEY UPDATE " + noopColumn + "=" + noopColumn;
    }

    @Override public String scheduleUpsert() {
        return "INSERT INTO wf_schedule (id,workflow,interval_millis,cron,context,next_fire_at,created_at) " +
                "VALUES (?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE workflow=VALUES(workflow), " +
                "interval_millis=VALUES(interval_millis), cron=VALUES(cron), " +
                "context=VALUES(context), next_fire_at=VALUES(next_fire_at)";
    }
}
