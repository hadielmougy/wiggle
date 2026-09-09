package com.wiggle.client.worker;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.dsl.ActivityHandler;
import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.core.InstanceView;
import com.wiggle.core.WorkflowDefinition;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The typed-activity registration model — one class per step, {@link Compensable} for the undo —
 * exercised through the same pure {@link HandlerBinder} seam as {@link HandlerBinderTest}, plus
 * one embedded end-to-end run mixing typed activities with lambda registration.
 */
class TypedActivityTest {

    private static WorkflowDefinition linear() {
        return Workflow.define("wf").step("capture-payment").gate("in-stock").effect("audit-log")
                .build().definition();
    }

    // ------------------------------------------------------------------ typed classes

    static final class CapturePayment implements Activity<Map<String, Object>>,
            Compensable<Map<String, Object>> {
        final AtomicReference<Object> refunded = new AtomicReference<>();
        public Map<String, Object> execute(Map<String, Object> ctx) {
            Map<String, Object> next = new LinkedHashMap<>(ctx);
            next.put("paymentRef", "pay-1");
            return next;
        }
        public void compensate(Map<String, Object> snapshot) {
            refunded.set(snapshot.get("paymentRef"));
        }
    }

    static final class InStock implements GateActivity<Map<String, Object>> {
        public boolean test(Map<String, Object> ctx) { return true; }
    }

    static final class AuditLog implements EffectActivity<Map<String, Object>> {
        public void apply(Map<String, Object> ctx) { }
    }

    // ------------------------------------------------------------------ binder-level

    @Test @DisplayName("class names match steps case/style-insensitively; kinds check out")
    void classNameMatching() throws Exception {
        CapturePayment cp = new CapturePayment();
        var r1 = HandlerBinder.bind(HandlerBinder.scanActivity("wf", cp, null), linear());
        var r2 = HandlerBinder.bind(HandlerBinder.scanActivity("wf", new InStock(), null), linear());
        var r3 = HandlerBinder.bind(HandlerBinder.scanActivity("wf", new AuditLog(), null), linear());

        assertEquals("capture-payment", r1.bindings().get(0).step(), "CapturePayment ↔ capture-payment");
        assertEquals("in-stock", r2.bindings().get(0).step());
        assertEquals("audit-log", r3.bindings().get(0).step());

        Object out = r1.bindings().get(0).handler().invoke(Map.of("a", 1L));
        assertTrue(out.toString().contains("paymentRef"), "task wrapper returns the whole next context");
        assertEquals(true, r2.bindings().get(0).handler().invoke(Map.of()));
        assertNull(r3.bindings().get(0).handler().invoke(Map.of()), "effect reports null");
    }

    @Test @DisplayName("a Compensable activity's undo is bound and receives the snapshot")
    void compensatorBound() throws Exception {
        CapturePayment cp = new CapturePayment();
        var r = HandlerBinder.bind(HandlerBinder.scanActivity("wf", cp, null), linear());
        ActivityHandler comp = r.bindings().get(0).compensator();
        assertNotNull(comp, "Compensable ⇒ the binding carries the undo");
        comp.invoke(Map.of("paymentRef", "pay-9"));
        assertEquals("pay-9", cp.refunded.get(), "the compensator saw the post-step snapshot");
        // non-Compensable activities carry none
        var r2 = HandlerBinder.bind(HandlerBinder.scanActivity("wf", new InStock(), null), linear());
        assertNull(r2.bindings().get(0).compensator());
    }

    @Test @DisplayName("a gate class bound to a task step (and vice versa) fails fast")
    void kindMismatch() {
        WorkflowDefinition def = Workflow.define("wf").step("in-stock").build().definition();
        assertThrows(IllegalStateException.class,
                () -> HandlerBinder.bind(HandlerBinder.scanActivity("wf", new InStock(), null), def),
                "boolean-shaped activity on a TASK node");
        WorkflowDefinition def2 = Workflow.define("wf").gate("capture-payment").build().definition();
        assertThrows(IllegalStateException.class,
                () -> HandlerBinder.bind(HandlerBinder.scanActivity("wf", new CapturePayment(), null), def2),
                "task-shaped activity on a PREDICATE node");
    }

    @Test @DisplayName("implementing two roles is ambiguous; a lambda needs an explicit name")
    void scanRejections() {
        // NB: the compiler already forces a name() override here (conflicting defaults) —
        // accidental double-roles don't even compile; the runtime check catches deliberate ones.
        class Both implements Activity<Map<String, Object>>, GateActivity<Map<String, Object>> {
            public Map<String, Object> execute(Map<String, Object> c) { return c; }
            public boolean test(Map<String, Object> c) { return true; }
            public String name() { return "both"; }
        }
        assertThrows(IllegalArgumentException.class,
                () -> HandlerBinder.scanActivity("wf", new Both(), null));
        Activity<Map<String, Object>> lambda = c -> c;
        assertThrows(IllegalArgumentException.class,
                () -> HandlerBinder.scanActivity("wf", lambda, null),
                "lambdas have no usable class name");
        // with an explicit name the same lambda is fine
        var r = HandlerBinder.bind(HandlerBinder.scanActivity("wf", lambda, "capture-payment"), linear());
        assertEquals("capture-payment", r.bindings().get(0).step());
    }

    // ------------------------------------------------------------------ end-to-end

    @Test @DisplayName("typed activities + a lambda run a workflow to COMPLETED on an embedded server")
    void endToEnd() throws Exception {
        Blueprint bp = Workflow.define("typed-flow")
                .step("capture-payment").gate("in-stock").step("summarise").effect("audit-log")
                .build();
        ServerConfig config = new ServerConfig(0, "typed-test", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(30),
                Duration.ofMillis(200), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
        try (WiggleServer server = new WiggleServer(config).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker worker = new Worker(client, "typed-w")
                     .register(bp)
                     .activities(new CapturePayment(), new InStock(), new AuditLog())
                     .activity("summarise", ctx -> {
                         Map<String, Object> next = new LinkedHashMap<>(ctx);
                         next.put("summary", "ok");
                         return next;
                     })) {
            worker.start();
            String id = client.start(bp, Map.of("orderId", "A-1"));
            InstanceView v = client.awaitCompletion(id, Duration.ofSeconds(10));
            assertEquals("COMPLETED", v.status());
            @SuppressWarnings("unchecked")
            Map<String, Object> ctx = (Map<String, Object>) v.context();
            assertEquals("pay-1", ctx.get("paymentRef"), "typed task ran");
            assertEquals("ok", ctx.get("summary"), "named lambda ran");
        }
    }

    @SuppressWarnings("unused")
    private static List<Object> unusedSilencer() { return List.of(); }
}
