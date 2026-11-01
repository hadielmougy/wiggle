package com.wiggle.jdbc;

import com.wiggle.server.store.Rows;
import com.wiggle.server.store.ScheduleRepository;
import org.springframework.jdbc.core.SqlParameterValue;

import java.sql.Types;
import java.util.List;
import java.util.Optional;

/** {@link ScheduleRepository} on {@code wf_schedule}. */
final class JdbcScheduleRepository extends JdbcRepository implements ScheduleRepository {

    JdbcScheduleRepository(JdbcStorage owner) {
        super(owner);
    }

    @Override public void putSchedule(Rows.Schedule s) {
        // Upsert by id: the engine reuses the existing id when a schedule for the same
        // workflow already exists, so a re-create updates the row rather than duplicating it.
        // The dialect supplies the SQL with positional parameters in insert-column order, so
        // this one stays positional: the statement text is the dialect's to spell, not ours.
        positional().update(dialect.scheduleUpsert(),
                s.id,
                s.workflow,
                s.intervalMillis,
                new SqlParameterValue(Types.VARCHAR, s.cron),
                s.context.json(),
                s.nextFireAt,
                s.createdAt);
    }

    @Override public void deleteSchedule(String id) {
        jdbc().update("DELETE FROM wf_schedule WHERE id=:id", params().add("id", id));
    }

    @Override public List<Rows.Schedule> dueSchedules(long now, int max) {
        return jdbc().query("SELECT * FROM wf_schedule WHERE next_fire_at<=:now ORDER BY next_fire_at LIMIT :max",
                params().add("now", now).add("max", max), (rs, row) -> JdbcStorage.readSchedule(rs));
    }

    @Override public boolean claimSchedule(String id, long expectedFireAt, long nextFireAt) {
        // Compare-and-set on the fire time: exactly one node moves a schedule forward.
        return jdbc().update("UPDATE wf_schedule SET next_fire_at=:next WHERE id=:id AND next_fire_at=:expected",
                params().add("next", nextFireAt).add("id", id).add("expected", expectedFireAt)) == 1;
    }

    @Override public List<Rows.Schedule> schedules() {
        return jdbc().query("SELECT * FROM wf_schedule ORDER BY id", (rs, row) -> JdbcStorage.readSchedule(rs));
    }

    @Override public Optional<Rows.Schedule> scheduleByWorkflow(String workflow) {
        return jdbc().query("SELECT * FROM wf_schedule WHERE workflow=:workflow",
                params().add("workflow", workflow), (rs, row) -> JdbcStorage.readSchedule(rs)).stream().findFirst();
    }
}
