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
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Typed activities via <b>factory methods</b> on a {@link Handlers @Handlers} class — a
 * zero-parameter method returning {@link Activity}/{@link GateActivity}/{@link EffectActivity} is
 * invoked once at scan and its result serves the step named by the method (or by
 * {@link Handles @Handles}); {@link Compensable} on the result binds the undo. Everything still
 * registers through the one {@code worker.handlers(...)} call, mixing freely with plain methods.
 */
class TypedActivityTest {

    /** The standalone typed activity — dependencies via constructor, undo alongside the do. */
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

    @Handlers("wf")
    static final class MixedHandlers {
        final CapturePayment capture = new CapturePayment();

        public boolean inStock(Map<String, Object> ctx) { return true; }      // plain gate method

        public Activity<Map<String, Object>> capturePayment() {               // factory -> "capture-payment"
            return capture;
        }

        public EffectActivity<Map<String, Object>> auditLog() {               // factory -> "audit-log"
            return ctx -> { };
        }
    }

    private static WorkflowDefinition linear() {
        return Workflow.define("wf").step("capture-payment").gate("in-stock").effect("audit-log")
                .build().definition();
    }

    // ------------------------------------------------------------------ binder-level

    @Test @DisplayName("factory methods register their activities under the method's name")
    void factoriesBindByMethodName() throws Exception {
        var r = HandlerBinder.bind(HandlerBinder.scan(new MixedHandlers()), linear());
        assertEquals(3, r.bindings().size());
        assertTrue(r.unserved().isEmpty());
        Map<String, HandlerBinder.Binding> byStep = new LinkedHashMap<>();
        r.bindings().forEach(b -> byStep.put(b.step(), b));

        Object out = byStep.get("capture-payment").handler().invoke(Map.of("a", 1L));
        assertTrue(out.toString().contains("paymentRef"), "typed task runs via the factory result");
        assertEquals(true, byStep.get("in-stock").handler().invoke(Map.of()), "plain method still binds");
        assertNull(byStep.get("audit-log").handler().invoke(Map.of()), "typed effect reports null");
    }

    @Test @DisplayName("a Compensable factory result carries the undo; it receives the snapshot")
    void compensatorBound() throws Exception {
        MixedHandlers h = new MixedHandlers();
        var r = HandlerBinder.bind(HandlerBinder.scan(h), linear());
        ActivityHandler comp = r.bindings().stream()
                .filter(b -> b.step().equals("capture-payment")).findFirst().orElseThrow().compensator();
        assertNotNull(comp, "Compensable ⇒ the binding carries the undo");
        comp.invoke(Map.of("paymentRef", "pay-9"));
        assertEquals("pay-9", h.capture.refunded.get(), "the compensator saw the post-step snapshot");
        // the plain method carries none
        assertNull(r.bindings().stream()
                .filter(b -> b.step().equals("in-stock")).findFirst().orElseThrow().compensator());
    }

    @Test @DisplayName("@Handles renames a handler away from its method name — plain and factory alike")
    void handlesAnnotation() throws Exception {
        @Handlers("wf")
        class Renamed {
            @Handles("capture-payment")
            public Map<String, Object> doTheCharge(Map<String, Object> ctx) { return ctx; }

            @Handles("in-stock")
            public GateActivity<Map<String, Object>> stockGate() { return ctx -> true; }

            public void auditLog(Map<String, Object> ctx) { }
        }
        var r = HandlerBinder.bind(HandlerBinder.scan(new Renamed()), linear());
        assertEquals(3, r.bindings().size());
        assertTrue(r.unserved().isEmpty(), "renamed methods served all three steps");
        assertEquals(true, r.bindings().stream()
                .filter(b -> b.step().equals("in-stock")).findFirst().orElseThrow()
                .handler().invoke(Map.of()));
    }

    @Test @DisplayName("scan rejections: parameterized factories, null factories, double roles, @Handles collisions")
    void scanRejections() {
        @Handlers("wf")
        class ParamFactory {
            public Activity<Map<String, Object>> capturePayment(String oops) { return c -> c; }
        }
        assertThrows(IllegalArgumentException.class, () -> HandlerBinder.scan(new ParamFactory()),
                "a factory must be zero-parameter");

        @Handlers("wf")
        class NullFactory {
            public Activity<Map<String, Object>> capturePayment() { return null; }
        }
        assertThrows(IllegalStateException.class, () -> HandlerBinder.scan(new NullFactory()));

        @Handlers("wf")
        class Collides {
            public Map<String, Object> capturePayment(Map<String, Object> c) { return c; }
            @Handles("capture-payment")
            public Map<String, Object> other(Map<String, Object> c) { return c; }
        }
        assertThrows(IllegalArgumentException.class, () -> HandlerBinder.scan(new Collides()),
                "@Handles colliding with a method name is ambiguous");
    }

    @Test @DisplayName("kind mismatches fail fast for factory results too")
    void kindMismatch() {
        @Handlers("wf")
        class GateOnTask {
            public GateActivity<Map<String, Object>> capturePayment() { return ctx -> true; }
        }
        assertThrows(IllegalStateException.class,
                () -> HandlerBinder.bind(HandlerBinder.scan(new GateOnTask()), linear()),
                "boolean-shaped activity on a TASK node");
    }

    // ------------------------------------------------------------------ end-to-end

    @Test @DisplayName("plain methods + factories + @Handles run a workflow to COMPLETED")
    void endToEnd() throws Exception {
        Blueprint bp = Workflow.define("wf")
                .step("capture-payment").gate("in-stock").step("summarise").effect("audit-log")
                .build();

        @Handlers("wf")
        class FlowHandlers {
            public boolean inStock(Map<String, Object> ctx) { return true; }

            public Activity<Map<String, Object>> capturePayment() {
                return new CapturePayment();
            }

            @Handles("summarise")
            public Activity<Map<String, Object>> buildSummary() {
                return ctx -> {
                    Map<String, Object> next = new LinkedHashMap<>(ctx);
                    next.put("summary", "ok");
                    return next;
                };
            }

            public EffectActivity<Map<String, Object>> auditLog() { return ctx -> { }; }
        }

        ServerConfig config = new ServerConfig(0, "typed-test", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(30),
                Duration.ofMillis(200), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
        try (WiggleServer server = new WiggleServer(config).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker worker = new Worker(client, "typed-w")
                     .register(bp)
                     .handlers(new FlowHandlers())) {
            worker.start();
            String id = client.start(bp, Map.of("orderId", "A-1"));
            InstanceView v = client.awaitCompletion(id, Duration.ofSeconds(10));
            assertEquals("COMPLETED", v.status());
            @SuppressWarnings("unchecked")
            Map<String, Object> ctx = (Map<String, Object>) v.context();
            assertEquals("pay-1", ctx.get("paymentRef"), "factory-produced task ran");
            assertEquals("ok", ctx.get("summary"), "@Handles-renamed factory ran");
        }
    }
}
