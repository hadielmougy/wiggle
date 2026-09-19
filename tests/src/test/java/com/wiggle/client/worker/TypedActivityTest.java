package com.wiggle.client.worker;

import com.wiggle.tests.TestPorts;
import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
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
 * Typed activities via <b>factory methods</b> on a {@link ForFlow @ForFlow} class — a
 * zero-parameter method returning {@link Activity}/{@link GateActivity}/{@link EffectActivity} is
 * invoked once at scan and its result serves the step named by the method (or by
 * {@link ForFlow @ForFlow}); {@link Compensable} on the result binds the undo. Everything still
 * registers through the one {@code worker.handlers(...)} call, mixing freely with plain methods.
 */
class TypedActivityTest {

    /** The steps this spec names; a worker binds them by name. */
    interface OneStep {
        void auditLog(Map<String, Object> ctx);
        // Both declarations of the same step name, because this file builds the node both ways: the
        // zero-arg factory declares an undo, the one-arg form does not. A method reference picks
        // between them by target type -- thenApplyCompensable takes the factory, thenApply the step.
        CompensableActivity<Map<String, Object>, Map<String, Object>> capturePayment();
        Map<String, Object> capturePayment(Map<String, Object> ctx);
        boolean inStock(Map<String, Object> ctx);
        Map<String, Object> summarise(Map<String, Object> ctx);
    }

    /** The standalone typed activity — dependencies via constructor, undo alongside the do. */
    static final class CapturePayment implements Activity<Map<String, Object>, Map<String, Object>>,
            Compensable<Map<String, Object>, Map<String, Object>> {
        final AtomicReference<Object> refunded = new AtomicReference<>();
        final AtomicReference<Object> undoKey = new AtomicReference<>();
        public Map<String, Object> execute(Map<String, Object> ctx) {
            Map<String, Object> next = new LinkedHashMap<>(ctx);
            next.put("paymentRef", "pay-1");
            return next;
        }
        public void compensate(Compensation<Map<String, Object>, Map<String, Object>> comp) {
            refunded.set(comp.result().get("paymentRef"));   // the step's own product: from result()
            undoKey.set(comp.input().get("idemKey"));        // undo-only data: from input(), never the context
        }
    }

    @ForFlow("wf")
    static final class MixedHandlers {
        final CapturePayment capture = new CapturePayment();

        public boolean inStock(Map<String, Object> ctx) { return true; }      // plain gate method

        public Activity<Map<String, Object>, Map<String, Object>> capturePayment() {               // factory -> "capturePayment"
            return capture;
        }

        public EffectActivity<Map<String, Object>> auditLog() {               // factory -> "auditLog"
            return ctx -> { };
        }
    }

    /** capturePayment declares its undo in its return type -- MixedHandlers supplies the factory. */
    private static WorkflowDefinition linear() {
        return FlowSpec.define("wf", 1, Map.class, OneStep.class, (f, s) -> f
                .thenApplyCompensable(s::capturePayment)
                .thenFilter(s::inStock)
                .thenAccept(s::auditLog)).definition();
    }

    /** Same shape, nothing compensable — for handler classes whose activities carry no undo. */
    private static WorkflowDefinition linearPlain() {
        return FlowSpec.define("wf", 1, Map.class, OneStep.class, (f, s) -> f
                .thenApply(s::capturePayment)
                .thenFilter(s::inStock)
                .thenAccept(s::auditLog)).definition();
    }


    @Test @DisplayName("factory methods register their activities under the method's name")
    void factoriesBindByMethodName() throws Exception {
        var r = HandlerBinder.bind(HandlerBinder.scan(new MixedHandlers()), linear());
        assertEquals(3, r.bindings().size());
        assertTrue(r.unserved().isEmpty());
        Map<String, HandlerBinder.Binding> byStep = new LinkedHashMap<>();
        r.bindings().forEach(b -> byStep.put(b.step(), b));

        Object out = byStep.get("capturePayment").handler().invoke(Map.of("a", 1L));
        assertTrue(out.toString().contains("paymentRef"), "typed task runs via the factory result");
        assertEquals(true, byStep.get("inStock").handler().invoke(Map.of()), "plain method still binds");
        assertNull(byStep.get("auditLog").handler().invoke(Map.of()), "typed effect reports null");
    }

    @Test @DisplayName("a Compensable factory result carries the undo; it sees BOTH snapshots")
    void compensatorBound() throws Exception {
        MixedHandlers h = new MixedHandlers();
        var r = HandlerBinder.bind(HandlerBinder.scan(h), linear());
        HandlerBinder.Compensator comp = r.bindings().stream()
                .filter(b -> b.step().equals("capturePayment")).findFirst().orElseThrow().compensator();
        assertNotNull(comp, "Compensable ⇒ the binding carries the undo");
        comp.invoke(Map.of("idemKey", "k-7"),                       // input snapshot
                    Map.of("paymentRef", "pay-9"));                 // result snapshot
        assertEquals("pay-9", h.capture.refunded.get(), "result(): the step's own product");
        assertEquals("k-7", h.capture.undoKey.get(), "input(): undo-only data, no context pollution");
        // the plain method carries none
        assertNull(r.bindings().stream()
                .filter(b -> b.step().equals("inStock")).findFirst().orElseThrow().compensator());
    }


    record Ord(String id) {}

    record Pay(String ref) {}

    interface TypedUndoSteps {
        CompensableActivity<Ord, Pay> charge();
    }

    @ForFlow("typed-undo")
    static final class TypedUndoH {
        final AtomicReference<Object> sawInput = new AtomicReference<>();
        final AtomicReference<Object> sawResult = new AtomicReference<>();

        public CompensableActivity<Ord, Pay> charge() {
            return new CompensableActivity<>() {
                @Override public Pay execute(Ord o) { return new Pay("pay-" + o.id()); }
                @Override public void compensate(Compensation<Ord, Pay> c) {
                    sawInput.set(c.input());
                    sawResult.set(c.result());
                }
            };
        }
    }

    @Test @DisplayName("each snapshot decodes into its own type: input as A, result as B")
    void compensatorSeesEachSnapshotAsItsOwnType() throws Exception {
        WorkflowDefinition def = FlowSpec.define("typed-undo", 1, Ord.class, TypedUndoSteps.class,
                (f, s) -> f.thenApplyCompensable(s::charge)).definition();

        TypedUndoH h = new TypedUndoH();
        HandlerBinder.Result r = HandlerBinder.bind(HandlerBinder.scan(h), def);
        HandlerBinder.Compensator comp = r.bindings().stream()
                .filter(b -> b.step().equals("charge")).findFirst().orElseThrow().compensator();
        assertNotNull(comp);

        // What the engine staged: the step's input and its result, as persisted JSON shapes. They are
        // different records, so decoding both into the parameter type would hand the undo a mangled
        // result rather than the Pay it produced.
        comp.invoke(Map.of("id", "A-1"), Map.of("ref", "pay-A-1"));

        assertEquals(new Ord("A-1"), h.sawInput.get(), "input() decodes into execute's parameter type");
        assertEquals(new Pay("pay-A-1"), h.sawResult.get(), "result() decodes into its return type");
    }

    @Test @DisplayName("@Handles renames a handler away from its method name — plain and factory alike")
    void handlesAnnotation() throws Exception {
        @ForFlow("wf")
        class Renamed {
            @Handles("capture-payment")
            public Map<String, Object> doTheCharge(Map<String, Object> ctx) { return ctx; }

            @Handles("in-stock")
            public GateActivity<Map<String, Object>> stockGate() { return ctx -> true; }

            public void auditLog(Map<String, Object> ctx) { }
        }
        var r = HandlerBinder.bind(HandlerBinder.scan(new Renamed()), linearPlain());
        assertEquals(3, r.bindings().size());
        assertTrue(r.unserved().isEmpty(), "renamed methods served all three steps");
        assertEquals(true, r.bindings().stream()
                .filter(b -> b.step().equals("inStock")).findFirst().orElseThrow()
                .handler().invoke(Map.of()));
    }

    @Test @DisplayName("scan rejections: parameterized factories, null factories, double roles, @Handles collisions")
    void scanRejections() {
        @ForFlow("wf")
        class ParamFactory {
            public Activity<Map<String, Object>, Map<String, Object>> capturePayment(String oops) { return c -> c; }
        }
        assertThrows(IllegalArgumentException.class, () -> HandlerBinder.scan(new ParamFactory()),
                "a factory must be zero-parameter");

        @ForFlow("wf")
        class NullFactory {
            public Activity<Map<String, Object>, Map<String, Object>> capturePayment() { return null; }
        }
        assertThrows(IllegalStateException.class, () -> HandlerBinder.scan(new NullFactory()));

        @ForFlow("wf")
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
        @ForFlow("wf")
        class GateOnTask {
            public GateActivity<Map<String, Object>> capturePayment() { return ctx -> true; }
        }
        assertThrows(IllegalStateException.class,
                () -> HandlerBinder.bind(HandlerBinder.scan(new GateOnTask()), linearPlain()),
                "boolean-shaped activity on a TASK node");
    }


    @Test @DisplayName("plain methods + factories + @ForFlow run a workflow to COMPLETED")
    void endToEnd() throws Exception {
        FlowSpec bp = FlowSpec.define("wf", 1, Map.class, OneStep.class, (f, s) -> f
                .thenApplyCompensable(s::capturePayment)
                .thenFilter(s::inStock)
                .thenApply(s::summarise)
                .thenAccept(s::auditLog));

        @ForFlow("wf")
        class FlowHandlers {
            public boolean inStock(Map<String, Object> ctx) { return true; }

            public Activity<Map<String, Object>, Map<String, Object>> capturePayment() {
                return new CapturePayment();
            }

            @Handles("summarise")
            public Activity<Map<String, Object>, Map<String, Object>> buildSummary() {
                return ctx -> {
                    Map<String, Object> next = new LinkedHashMap<>(ctx);
                    next.put("summary", "ok");
                    return next;
                };
            }

            public EffectActivity<Map<String, Object>> auditLog() { return ctx -> { }; }
        }

        ServerConfig config = new ServerConfig(TestPorts.free(), "typed-test", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(30),
                Duration.ofMillis(200), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
        try (WiggleServer server = new WiggleServer(config).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker worker = new Worker(client, "typed-w")
                     
                     .registerHandler(new FlowHandlers())) {
            client.register(bp);
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
