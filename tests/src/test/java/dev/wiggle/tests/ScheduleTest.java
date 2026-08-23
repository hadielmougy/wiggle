package dev.wiggle.tests;

import dev.wiggle.client.dsl.Blueprint;
import dev.wiggle.client.dsl.Workflow;
import dev.wiggle.core.Json;
import dev.wiggle.server.engine.DefinitionRegistry;
import dev.wiggle.server.engine.WorkflowEngine;
import dev.wiggle.server.store.InMemoryStorage;
import dev.wiggle.server.store.Rows;
import dev.wiggle.server.store.Storage;
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

    private static Blueprint<Map<String, Object>> probe() {
        return Workflow.defineJson("sched-probe").step("work", ctx -> ctx).build();
    }

    @Test @DisplayName("a due schedule fires exactly one instance and re-arms one interval ahead")
    void firesAndRearms() throws Exception {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            WorkflowEngine engine = engine(storage);
            Blueprint<Map<String, Object>> bp = probe();
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
}
