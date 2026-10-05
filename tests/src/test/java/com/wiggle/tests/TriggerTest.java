package com.wiggle.tests;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.core.InstanceView;
import com.wiggle.core.Json;
import com.wiggle.server.engine.DefinitionRegistry;
import com.wiggle.server.engine.EngineException;
import com.wiggle.server.engine.WorkflowEngine;
import com.wiggle.server.store.InMemoryStorage;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.Storage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Triggers: starts on other instances' events, exactly once, from the event log. */
class TriggerTest {

    interface OneStep {
        Map<String, Object> work(Map<String, Object> ctx);
    }

    private static WorkflowEngine engine(Storage storage) {
        return new WorkflowEngine(storage, new DefinitionRegistry(storage), 30_000);
    }

    private static FlowSpec flow(String name) {
        return FlowSpec.define(name, 1, Map.class, OneStep.class, (f, s) -> f.thenApply(s::work));
    }

    /** Dispatches until nothing is left, past the feed's visibility window. */
    private static int drain(WorkflowEngine engine) throws InterruptedException {
        int total = 0;
        for (int round = 0; round < 50; round++) {
            Thread.sleep(70);
            int n = engine.dispatchTriggers(100);
            if (n == 0) return total;
            total += n;
        }
        return total;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> triggerOf(InstanceView v) {
        return (Map<String, Object>) Json.asObject(v.context()).get("trigger");
    }

    @Test @DisplayName("a cancelled source starts the target once, with the source's context and what fired it")
    void firesOnceWithContext() throws Exception {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            WorkflowEngine engine = engine(storage);
            engine.register(flow("trg-order").definition());
            engine.register(flow("trg-cleanup").definition());
            String trigger = engine.createTrigger("trg-cleanup", "trg-order", List.of("wf.cancelled"), true);

            String order = engine.start("trg-order", null, Map.of("orderId", "o-1"), "order-1");
            engine.cancel(order, "customer changed their mind");
            drain(engine);
            assertEquals(0, engine.dispatchTriggers(100), "nothing is dispatched twice");

            List<InstanceView> started = engine.list("trg-cleanup", null, 10);
            assertEquals(1, started.size());
            Map<String, Object> ctx = Json.asObject(started.get(0).context());
            assertEquals("o-1", ctx.get("orderId"), "the source's context is copied");
            Map<String, Object> fired = triggerOf(started.get(0));
            assertEquals(trigger, fired.get("triggerId"));
            assertEquals("wf.cancelled", fired.get("event"));
            assertEquals(order, fired.get("instanceId"));
            assertEquals("trg-order", fired.get("workflow"));
            assertEquals("order-1", fired.get("correlationId"));
            assertEquals("customer changed their mind",
                    Json.asObject(fired.get("payload")).get("reason"), "the event's payload rides along");
            assertEquals(1, storage.inTx(tx -> tx.findByCorrelation("trigger:" + trigger + ":" + order, 10)).size(),
                    "correlated to the trigger and the source instance");
        }
    }

    @Test @DisplayName("an event appended before the trigger existed does not fire it, and other event types do not")
    void onlyNewMatchingEvents() throws Exception {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            WorkflowEngine engine = engine(storage);
            engine.register(flow("trg-src").definition());
            engine.register(flow("trg-dst").definition());
            String early = engine.start("trg-src", null, Map.of(), null);
            engine.cancel(early, "before the trigger");
            Thread.sleep(5);

            engine.createTrigger("trg-dst", "trg-src", List.of("wf.cancelled"), false);
            engine.start("trg-src", null, Map.of(), null);
            drain(engine);
            assertEquals(0, engine.list("trg-dst", null, 10).size(),
                    "the earlier cancel predates the trigger, and wf.started is not one of its types");

            String late = engine.start("trg-src", null, Map.of("secret", 1), null);
            engine.cancel(late, "after");
            drain(engine);
            List<InstanceView> started = engine.list("trg-dst", null, 10);
            assertEquals(1, started.size());
            assertNull(Json.asObject(started.get(0).context()).get("secret"), "without includeContext nothing is copied");
        }
    }

    @Test @DisplayName("'*' listens to every other workflow, never its own target")
    void wildcardSkipsItsOwnTarget() throws Exception {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            WorkflowEngine engine = engine(storage);
            engine.register(flow("trg-a").definition());
            engine.register(flow("trg-b").definition());
            engine.register(flow("trg-audit").definition());
            engine.createTrigger("trg-audit", "*", List.of("wf.started"), false);

            engine.start("trg-a", null, Map.of(), null);
            engine.start("trg-b", null, Map.of(), null);
            drain(engine);
            assertEquals(2, engine.list("trg-audit", null, 10).size(),
                    "one audit per other workflow's start, and none for the audits' own starts");
        }
    }

    @Test @DisplayName("a cycle of triggers stops after the chain depth cap")
    void cyclesAreCapped() throws Exception {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            WorkflowEngine engine = engine(storage);
            engine.register(flow("trg-ping").definition());
            engine.register(flow("trg-pong").definition());
            engine.createTrigger("trg-pong", "trg-ping", List.of("wf.started"), false);
            engine.createTrigger("trg-ping", "trg-pong", List.of("wf.started"), false);

            engine.start("trg-ping", null, Map.of(), null);
            drain(engine);
            int total = engine.list("trg-ping", null, 100).size() + engine.list("trg-pong", null, 100).size();
            assertEquals(1 + 16, total, "the first start, then sixteen triggered ones");
            int deepest = engine.list("trg-ping", null, 100).stream()
                    .map(TriggerTest::triggerOf).filter(java.util.Objects::nonNull)
                    .mapToInt(t -> ((Number) t.get("depth")).intValue()).max().orElse(0);
            assertTrue(deepest <= 16, "depth is carried and never passes the cap: " + deepest);
        }
    }

    @Test @DisplayName("the dispatch position is a compare-and-set: an overlapping leader's stale batch fires nothing")
    void stalePositionCannotDoubleFire() throws Exception {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            WorkflowEngine engine = engine(storage);
            engine.register(flow("trg-x").definition());
            engine.register(flow("trg-y").definition());
            engine.createTrigger("trg-y", "trg-x", List.of("wf.started"), false);
            long stale = storage.inTx(tx -> tx.triggerCursor());

            engine.start("trg-x", null, Map.of(), null);
            drain(engine);
            assertEquals(1, engine.list("trg-y", null, 10).size());
            boolean moved = storage.<Boolean>inTx(tx -> tx.moveTriggerCursor(stale, stale + 1, System.currentTimeMillis()));
            assertFalse(moved,
                    "a second leader holding the old position loses the move");
        }
    }

    @Test @DisplayName("upsert on (workflow, source); validation; deleting the last trigger drops the positions")
    void lifecycleAndValidation() throws Exception {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            WorkflowEngine engine = engine(storage);
            engine.register(flow("trg-p").definition());
            engine.register(flow("trg-q").definition());

            String id = engine.createTrigger("trg-q", "trg-p", List.of("wf.failed"), false);
            assertEquals(id, engine.createTrigger("trg-q", "trg-p", List.of("wf.completed", "wf.failed"), true),
                    "the same route updates in place");
            Rows.Trigger t = engine.triggers().getFirst();
            assertEquals(List.of("wf.completed", "wf.failed"), t.eventTypes);
            assertTrue(t.includeContext);
            assertNotNull(storage.inTx(tx -> tx.triggerCursor()), "the position exists before the trigger fires");

            assertEquals(404, assertThrows(EngineException.class,
                    () -> engine.createTrigger("nope", "trg-p", List.of("wf.failed"), false)).statusCode());
            assertEquals(400, assertThrows(EngineException.class,
                    () -> engine.createTrigger("trg-q", "trg-p", List.of("wf.exploded"), false)).statusCode());
            assertEquals(400, assertThrows(EngineException.class,
                    () -> engine.createTrigger("trg-q", "trg-p", List.of(), false)).statusCode());
            assertEquals(400, assertThrows(EngineException.class,
                    () -> engine.createTrigger("trg-q", "trg-q", List.of("wf.failed"), false)).statusCode());

            assertTrue(engine.deleteTrigger(id));
            assertFalse(engine.deleteTrigger(id), "a second delete finds nothing");
            assertTrue(engine.triggers().isEmpty());
            assertNull(storage.inTx(tx -> tx.triggerCursor()), "with no trigger left, no position is kept");
        }
    }
}