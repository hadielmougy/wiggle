package com.wiggle.server.engine;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.client.worker.Activity;
import com.wiggle.client.worker.Compensable;
import com.wiggle.client.worker.Compensation;
import com.wiggle.client.worker.Handlers;
import com.wiggle.client.worker.PermanentActivityException;
import com.wiggle.client.worker.Worker;
import com.wiggle.core.InstanceView;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The saga engine phase end-to-end: a failed instance runs its declared compensable steps' undos
 * in reverse completion order — real leased tasks through the normal worker dispatch path — and
 * lands COMPENSATED; a failing compensator lands COMPENSATION_FAILED; an undeclared flow still
 * just FAILS. Each compensator receives its OWN step's input/result snapshots (replace semantics
 * make the latest context unusable for undo — see docs/saga-compensation.md).
 */
class SagaCompensationTest {

    record Undo(String step, Object input, Object result) {}

    static final class Recording {
        final List<Undo> undos = new CopyOnWriteArrayList<>();
    }

    /** A compensable step: marks the context, records its undo invocation with both snapshots. */
    static Activity<Map<String, Object>> compensableStep(String mark, Recording rec, boolean undoFails) {
        final class Step implements Activity<Map<String, Object>>, Compensable<Map<String, Object>> {
            public Map<String, Object> execute(Map<String, Object> ctx) {
                Map<String, Object> next = new LinkedHashMap<>(ctx);
                next.put(mark, true);
                return next;
            }
            public void compensate(Compensation<Map<String, Object>> c) {
                if (undoFails) throw new PermanentActivityException("undo of " + mark + " broke");
                rec.undos.add(new Undo(mark, c.input(), c.result()));
            }
        }
        return new Step();
    }

    private static ServerConfig config() {
        return new ServerConfig(0, "saga-test", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(30),
                Duration.ofMillis(200), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    private static InstanceView run(Blueprint bp, Object handlers) throws Exception {
        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker worker = new Worker(client, "saga-w").register(bp).handlers(handlers)) {
            worker.start();
            String id = client.start(bp, Map.of("orderId", "A-1"));
            return client.awaitCompletion(id, Duration.ofSeconds(15));
        }
    }

    @Test @Timeout(30)
    @DisplayName("a failed instance compensates its completed steps in reverse order -> COMPENSATED")
    void reverseOrderSaga() throws Exception {
        Recording rec = new Recording();
        Blueprint bp = Workflow.define("saga")
                .step("reserve").compensate()
                .step("capture").compensate()
                .step("boom")
                .build();

        @Handlers("saga")
        class H {
            public Activity<Map<String, Object>> reserve() { return compensableStep("reserved", rec, false); }
            public Activity<Map<String, Object>> capture() { return compensableStep("captured", rec, false); }
            public Map<String, Object> boom(Map<String, Object> ctx) {
                throw new PermanentActivityException("downstream exploded");
            }
        }

        InstanceView v = run(bp, new H());
        assertEquals("COMPENSATED", v.status());
        assertNotNull(v.error());
        assertTrue(v.error().contains("downstream exploded"), v.error());

        assertEquals(List.of("captured", "reserved"),
                rec.undos.stream().map(Undo::step).toList(), "reverse completion order");

        // per-step snapshots, not the latest context:
        @SuppressWarnings("unchecked")
        Map<String, Object> capResult = (Map<String, Object>) rec.undos.get(0).result();
        @SuppressWarnings("unchecked")
        Map<String, Object> capInput = (Map<String, Object>) rec.undos.get(0).input();
        @SuppressWarnings("unchecked")
        Map<String, Object> resResult = (Map<String, Object>) rec.undos.get(1).result();
        assertTrue((Boolean) capResult.get("captured"), "capture's result() has its own product");
        assertTrue((Boolean) capResult.get("reserved"), "…and everything before it");
        assertFalse(capInput.containsKey("captured"), "capture's input() predates its own step");
        assertTrue(capInput.containsKey("reserved"));
        assertFalse(resResult.containsKey("captured"), "reserve's result() predates capture");
    }

    @Test @Timeout(30)
    @DisplayName("locally-chained (LOCAL_SYNC) compensable steps capture snapshots and compensate too")
    void localSyncSaga() throws Exception {
        Recording rec = new Recording();
        Blueprint bp = Workflow.define("saga-local")
                .execution(com.wiggle.core.ExecutionMode.LOCAL_SYNC)
                .step("reserve").compensate()
                .step("capture").compensate()
                .step("boom")
                .build();

        @Handlers("saga-local")
        class H {
            public Activity<Map<String, Object>> reserve() { return compensableStep("reserved", rec, false); }
            public Activity<Map<String, Object>> capture() { return compensableStep("captured", rec, false); }
            public Map<String, Object> boom(Map<String, Object> ctx) {
                throw new PermanentActivityException("downstream exploded");
            }
        }

        InstanceView v = run(bp, new H());
        assertEquals("COMPENSATED", v.status());
        assertEquals(List.of("captured", "reserved"),
                rec.undos.stream().map(Undo::step).toList(),
                "applyRun captured both steps' snapshots despite local chaining");
    }

    @Test @Timeout(30)
    @DisplayName("no declared compensation -> plain FAILED, exactly as before")
    void undeclaredStillFails() throws Exception {
        Blueprint bp = Workflow.define("plain-fail").step("work").step("boom").build();
        @Handlers("plain-fail")
        class H {
            public Map<String, Object> work(Map<String, Object> ctx) { return ctx; }
            public Map<String, Object> boom(Map<String, Object> ctx) {
                throw new PermanentActivityException("nope");
            }
        }
        InstanceView v = run(bp, new H());
        assertEquals("FAILED", v.status());
    }

    @Test @Timeout(30)
    @DisplayName("a compensator that fails permanently lands COMPENSATION_FAILED, loudly")
    void compensatorFailure() throws Exception {
        Recording rec = new Recording();
        Blueprint bp = Workflow.define("bad-undo")
                .step("reserve").compensate()
                .step("boom")
                .build();
        @Handlers("bad-undo")
        class H {
            public Activity<Map<String, Object>> reserve() { return compensableStep("reserved", rec, true); }
            public Map<String, Object> boom(Map<String, Object> ctx) {
                throw new PermanentActivityException("downstream exploded");
            }
        }
        InstanceView v = run(bp, new H());
        assertEquals("COMPENSATION_FAILED", v.status());
        assertTrue(v.error().contains("compensator"), v.error());
        assertTrue(v.error().contains("reserve"), v.error());
    }
}
