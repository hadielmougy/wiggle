package dev.wiggle.postgres;

import dev.wiggle.jdbc.Dialect;

/**
 * H2 in PostgreSQL-compatibility mode: the embedded database used for development and the test
 * suite. It accepts the canonical PostgreSQL DDL and {@code ON CONFLICT} syntax, but cannot run
 * {@code FOR UPDATE SKIP LOCKED}, so the task claim falls back to the portable compare-and-set
 * path. It has no cross-node migration lock (development and tests are single-node).
 */
public final class H2Dialect implements Dialect {

    @Override public String id() { return "h2"; }

    @Override public String firstRow() { return "FETCH FIRST 1 ROWS ONLY"; }

    @Override public String insertIgnore(String insertSql, String noopColumn) {
        return insertSql + " ON CONFLICT DO NOTHING";
    }

    @Override public String scheduleUpsert() {
        return "INSERT INTO wf_schedule (id,workflow,interval_millis,cron,context,next_fire_at,created_at) " +
                "VALUES (?,?,?,?,?,?,?) ON CONFLICT (id) DO UPDATE SET workflow=EXCLUDED.workflow, " +
                "interval_millis=EXCLUDED.interval_millis, cron=EXCLUDED.cron, " +
                "context=EXCLUDED.context, next_fire_at=EXCLUDED.next_fire_at";
    }
}
