package dev.wiggle.oracle;

import dev.wiggle.jdbc.Dialect;

import java.sql.SQLException;

/**
 * Oracle Database. Several things differ from the canonical PostgreSQL SQL:
 *
 * <ul>
 *   <li><b>DDL</b> — Oracle (pre-23c) has no {@code IF NOT EXISTS}, no {@code TEXT} type and no
 *       {@code BIGINT}; {@link #ddl} strips the former and maps {@code TEXT}→{@code CLOB},
 *       {@code BIGINT}→{@code NUMBER(19)}. It also drops the {@code NOT NULL} on {@code join_stack}
 *       because Oracle stores the empty-string sentinel as {@code NULL} (the store normalises it
 *       back to {@code ""} on read).</li>
 *   <li><b>Row limiting</b> — {@code LIMIT ?} becomes {@code FETCH FIRST ? ROWS ONLY}.</li>
 *   <li><b>Task claim</b> — Oracle rejects the row-limiting clause together with {@code FOR UPDATE},
 *       so SKIP LOCKED is not usable for the bounded claim; the portable compare-and-set path is
 *       used instead (correct, if slightly less efficient under heavy contention).</li>
 *   <li><b>Upserts</b> — no inline conflict-ignore, so idempotent inserts run plain and a genuine
 *       duplicate-key error is swallowed; the schedule upsert is a {@code MERGE}.</li>
 * </ul>
 */
public final class OracleDialect implements Dialect {

    @Override public String id() { return "oracle"; }

    @Override public String firstRow() { return "FETCH FIRST 1 ROWS ONLY"; }

    @Override public String limit(String sql) {
        return sql.replace("LIMIT ?", "FETCH FIRST ? ROWS ONLY");
    }

    @Override public String ddl(String canonicalSql) {
        String s = canonicalSql.replaceAll("(?i)\\s+IF NOT EXISTS", "");
        // Oracle's ALTER TABLE has no COLUMN keyword: it's `ADD col type`, not `ADD COLUMN col type`.
        s = s.replaceAll("(?i)\\bADD COLUMN\\b", "ADD");
        s = s.replaceAll("\\bTEXT\\b", "CLOB");
        s = s.replaceAll("\\bBIGINT\\b", "NUMBER(19)");
        // Oracle treats '' as NULL, so the NOT-NULL '' sentinel on join_stack would violate the
        // constraint; make the column nullable and let the store normalise NULL back to "".
        s = s.replaceAll("join_stack\\s+VARCHAR\\(1000\\)\\s+NOT NULL", "join_stack VARCHAR(1000)");
        return s;
    }

    @Override public boolean isBenignMigrationError(SQLException e) {
        for (SQLException cur = e; cur != null; cur = cur.getNextException()) {
            int code = cur.getErrorCode();
            if (code == 955    // ORA-00955: name is already used by an existing object
                    || code == 1430) {  // ORA-01430: column being added already exists in table
                return true;
            }
        }
        return false;
    }

    @Override public String insertIgnore(String insertSql, String noopColumn) {
        return insertSql;  // no inline ignore; a real duplicate is caught via isDuplicateKey
    }

    @Override public boolean isDuplicateKey(SQLException e) {
        for (SQLException cur = e; cur != null; cur = cur.getNextException()) {
            if (cur instanceof java.sql.SQLIntegrityConstraintViolationException) return true;
            if (cur.getErrorCode() == 1) return true;  // ORA-00001: unique constraint violated
        }
        return false;
    }

    @Override public String scheduleUpsert() {
        return "MERGE INTO wf_schedule d USING (SELECT ? id, ? workflow, ? interval_millis, ? cron, " +
                "? context, ? next_fire_at, ? created_at FROM dual) s ON (d.id = s.id) " +
                "WHEN MATCHED THEN UPDATE SET d.workflow=s.workflow, d.interval_millis=s.interval_millis, " +
                "d.cron=s.cron, d.context=s.context, d.next_fire_at=s.next_fire_at " +
                "WHEN NOT MATCHED THEN INSERT (id,workflow,interval_millis,cron,context,next_fire_at,created_at) " +
                "VALUES (s.id,s.workflow,s.interval_millis,s.cron,s.context,s.next_fire_at,s.created_at)";
    }
}
