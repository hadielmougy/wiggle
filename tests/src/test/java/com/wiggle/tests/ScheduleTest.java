package com.wiggle.tests;

import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.core.Json;
import com.wiggle.server.engine.DefinitionRegistry;
import com.wiggle.server.engine.WorkflowEngine;
import com.wiggle.server.store.InMemoryStorage;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.Storage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Recurring schedules: interval firing, exactly-once claims, lifecycle, validation. */
class ScheduleTest {

    private static WorkflowEngine engine(Storage storage) {
        return new WorkflowEngine(storage, new DefinitionRegistry(storage), 30_000);
    }

    private static Blueprint probe() {
        return Workflow.define("sched-probe").step("work").build();
    }

    @Test @DisplayName("a due schedule fires exactly one instance and re-arms one interval ahead")
    void firesAndRearms() throws Exception {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            WorkflowEngine engine = engine(storage);
            Blueprint bp = probe();
            engine.register(bp.definition());

            String id = engine.createSchedule("sched-probe", Duration.ofMillis(50), Map.of("from", "schedule"));
            assertEquals(0, engine.fireDueSchedules(10), "not due yet");
            Thread.sleep(80);

            assertEquals(1, engine.fireDueSchedules(10), "fires once when due");
            assertEquals(0, engine.fireDueSchedules(10), "and is re-armed, not re-fired");

            var instances = engine.list("sched-probe", null, 10);
            assertEquals(1, instances.size());
            assertEquals("schedule", Json.asObject(instances.get(0).context()).get("from"),
                    "the schedule's context seeds the instance");

            Rows.Schedule sched = engine.schedules().get(0);
            assertEquals(id, sched.id);
            assertTrue(sched.nextFireAt > System.currentTimeMillis() - 5, "re-armed into the future");
        }
    }

    @Test @DisplayName("the fire-time claim is a compare-and-set: a stale sweep cannot double-fire")
    void staleClaimCannotDoubleFire() throws Exception {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            WorkflowEngine engine = engine(storage);
            engine.register(probe().definition());
            engine.createSchedule("sched-probe", Duration.ofMillis(50), Map.of());
            Thread.sleep(80);

            Rows.Schedule stale = storage.inTx(tx -> tx.dueSchedules(System.currentTimeMillis(), 10)).get(0);
            assertEquals(1, engine.fireDueSchedules(10), "first sweep fires");
            // A second actor holding the stale snapshot loses the CAS and must not start anything.
            boolean claimed = storage.inTx(tx ->
                    tx.claimSchedule(stale.id, stale.nextFireAt, System.currentTimeMillis() + 1000));
            assertTrue(!claimed, "the stale fire time no longer matches");
            assertEquals(1, engine.list("sched-probe", null, 10).size(), "still exactly one instance");
        }
    }

    @Test @DisplayName("deleting a schedule stops it; creating one for an unknown workflow fails")
    void lifecycleAndValidation() throws Exception {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            WorkflowEngine engine = engine(storage);
            engine.register(probe().definition());

            String id = engine.createSchedule("sched-probe", Duration.ofMillis(30), Map.of());
            engine.deleteSchedule(id);
            Thread.sleep(60);
            assertEquals(0, engine.fireDueSchedules(10), "a deleted schedule never fires");
            assertTrue(engine.schedules().isEmpty());

            assertThrows(RuntimeException.class,
                    () -> engine.createSchedule("no-such-workflow", Duration.ofMillis(30), Map.of()),
                    "the workflow must be registered");
            assertThrows(RuntimeException.class,
                    () -> engine.createSchedule("sched-probe", Duration.ZERO, Map.of()),
                    "the interval must be positive");
        }
    }

    @Test @DisplayName("a cron schedule arms at the expression's next UTC match and re-arms by cron on fire")
    void cronSchedule() throws Exception {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            WorkflowEngine engine = engine(storage);
            engine.register(probe().definition());

            long before = System.currentTimeMillis();
            String id = engine.createCronSchedule("sched-probe", "0 3 * * *", Map.of("via", "cron"));
            Rows.Schedule sched = engine.schedules().get(0);
            assertEquals("0 3 * * *", sched.cron);
            assertEquals(com.wiggle.core.Cron.parse("0 3 * * *").next(before), sched.nextFireAt,
                    "armed at the cron's next match, not now+interval");
            assertEquals(0, engine.fireDueSchedules(10), "3am UTC is not due right now");

            // Backdate the fire time through the CAS (the only mutator) to force it due.
            long past = System.currentTimeMillis() - 10;
            boolean backdated = storage.inTx(tx -> tx.claimSchedule(id, sched.nextFireAt, past));
            assertTrue(backdated);
            assertEquals(1, engine.fireDueSchedules(10), "fires once due");
            assertEquals(1, engine.list("sched-probe", null, 10).size());
            assertEquals("cron", Json.asObject(engine.list("sched-probe", null, 10).get(0).context()).get("via"));

            long rearmed = engine.schedules().get(0).nextFireAt;
            var t = java.time.Instant.ofEpochMilli(rearmed).atZone(java.time.ZoneOffset.UTC);
            assertTrue(rearmed > System.currentTimeMillis() - 60_000, "re-armed into the future");
            assertEquals(3, t.getHour(), "re-armed by the cron, not by an interval");
            assertEquals(0, t.getMinute());

            assertThrows(RuntimeException.class,
                    () -> engine.createCronSchedule("sched-probe", "not a cron", Map.of()),
                    "invalid expressions are rejected");
        }
    }
}
