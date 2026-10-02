package com.wiggle.postgres;

import com.wiggle.jdbc.Dialect;

/**
 * H2 in PostgreSQL-compatibility mode: the embedded database used for development and the test
 * suite, not a deployment target. It takes the store's DDL verbatim and almost all of its SQL, so it
 * inherits nearly every {@link Dialect} default. What it does not inherit is the two capabilities
 * PostgreSQL declares: it has no {@code FOR UPDATE SKIP LOCKED} and no {@code RETURNING}, so the
 * task claim falls back to compare-and-set, and no cheap cross-node migration lock, which is moot
 * because development and tests are single-node.
 *
 * <p>Being so nearly the same statements is the point rather than an accident: it is what makes H2 a
 * faithful stand-in for the suite. The one statement it has to spell differently is the schedule
 * upsert -- H2 accepts {@code ON CONFLICT DO NOTHING} but not {@code DO UPDATE}, so that one is a
 * standard {@code MERGE} instead.
 */
public final class H2Dialect implements Dialect {

    @Override public String id() { return "h2"; }

    /**
     * The schedule upsert as a standard {@code MERGE}: H2's {@code ON CONFLICT} support stops at
     * {@code DO NOTHING}, which covers {@link #insertIgnore} but not an upsert that has to update.
     * Same seven parameters in the same insert-column order, and the same columns updated -- so
     * {@code created_at} stays what the row was first written with, as it does on PostgreSQL. The
     * shorter {@code MERGE INTO ... KEY(id) VALUES} would overwrite it.
     */
    @Override public String scheduleUpsert() {
        return """
                MERGE INTO wf_schedule AS t
                  USING (VALUES (?,?,?,?,?,?,?))
                    AS s(id,workflow,interval_millis,cron,context,next_fire_at,created_at)
                  ON t.id = s.id
                  WHEN MATCHED THEN UPDATE SET t.workflow=s.workflow,
                       t.interval_millis=s.interval_millis, t.cron=s.cron, t.context=s.context,
                       t.next_fire_at=s.next_fire_at
                  WHEN NOT MATCHED THEN
                       INSERT (id,workflow,interval_millis,cron,context,next_fire_at,created_at)
                       VALUES (s.id,s.workflow,s.interval_millis,s.cron,s.context,s.next_fire_at,
                               s.created_at)
                """;
    }
}
