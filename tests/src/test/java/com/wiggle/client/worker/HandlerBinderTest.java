package com.wiggle.client.worker;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.flow.Wiggle;
import com.wiggle.core.WorkflowDefinition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link HandlerBinder} in isolation — the binder is pure (graph in → bindings out), so every
 * signature rule is testable with no server, no worker, no I/O. Wrapper invocations are wrapped
 * in {@link Step#begin}/{@link Step#end} to replicate the worker's runtime contract.
 */
class HandlerBinderTest {

    /** The steps this spec names; a worker binds them by name. */
    interface OneStep {
        void log(Map<String, Object> ctx);
        boolean ok(Map<String, Object> ctx);
        Map<String, Object> served(Map<String, Object> ctx);
        Map<String, Object> someoneElses(Map<String, Object> ctx);
        Map<String, Object> work(Map<String, Object> ctx);
    }


    @ForFlow("")
    static final class BlankH {
        public Map<String, Object> a(Map<String, Object> c) { return c; }
    }

    /** No @ForFlow at all: legal now, but the name has to arrive some other way. */
    static final class UnnamedH {
        public Map<String, Object> a(Map<String, Object> c) { return c; }
    }

    @Test @DisplayName("scan rejects a blank workflow name, and leaves a missing one for the caller")
    void scanWorkflowName() {
        // An annotation that names nothing is a mistake the binder can see, so it still throws.
        assertThrows(IllegalArgumentException.class, () -> HandlerBinder.scan(new BlankH()));

        // No annotation is not a mistake the binder can see: the name may be coming from
        // registerHandler(name, handlers). So scan reports "unknown" rather than failing.
        HandlerBinder.HandlerSet set = HandlerBinder.scan(new UnnamedH());
        assertNull(set.workflow(), "an unannotated object has no workflow of its own");
        assertEquals("orders", set.withFlowName("orders").workflow(),
                "and the caller's name is what supplies it");
    }

    @Test @DisplayName("registerHandler: the workflow name must come from the annotation or the call")
    void registerHandlerNeedsAWorkflowName() {
        // Neither source has one -- rejected here, at the call that made the mistake, rather than
        // later when the worker tries to fetch a graph called null.
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> new Worker(null, "w").registerHandler(new UnnamedH()));
        assertTrue(e.getMessage().contains("flow name"), e.getMessage());

        // Either source on its own is enough.
        assertDoesNotThrow(() -> new Worker(null, "w").registerHandler("orders", new UnnamedH()),
                "the name given at the call site stands in for the annotation");
        assertDoesNotThrow(() -> new Worker(null, "w").registerHandler(new ForkCombineH()),
                "and an annotated object needs no name");
    }

    @Test @DisplayName("scan rejects two methods whose names collide under case-folding")
    void scanRejectsCollisions() {
        IllegalArgumentException e =
                assertThrows(IllegalArgumentException.class, () -> HandlerBinder.scan(new CollidingH()));
        assertTrue(e.getMessage().contains("ambiguous"), e.getMessage());
    }

    @ForFlow("wf")
    static final class CollidingH {
        public Map<String, Object> inStock(Map<String, Object> c) { return c; }
        public Map<String, Object> instock(Map<String, Object> c) { return c; }
    }

    @Test @DisplayName("scan collects @Decode decoders and ignores zero-parameter helpers")
    void scanCollectsDecodersAndSkipsHelpers() {
        HandlerBinder.HandlerSet set = HandlerBinder.scan(new DecoderH());
        assertTrue(set.decoders().containsKey(Map.class), "decoder registered by return type");
        assertTrue(set.byName().containsKey("work"));
        assertTrue(!set.byName().containsKey("helper"), "0-param method is a helper, not a handler");
    }

    @ForFlow("wf")
    static final class DecoderH {
        @Decode public Map<String, Object> load(Map<String, Object> raw) { return raw; }
        public Map<String, Object> work(Map<String, Object> c) { return c; }
        public String helper() { return "not a handler"; }
    }


    private static WorkflowDefinition linear() {
        return FlowSpec.define("wf", 1, Map.class, OneStep.class, (f, s) -> f
                .thenApply(s::work)
                .thenFilter(s::ok)
                .thenAccept(s::log)).definition();
    }

    @Test @DisplayName("bind: task returns whole context, gate returns boolean, effect returns null")
    void bindsAllKinds() throws Exception {
        HandlerBinder.Result r = HandlerBinder.bind(HandlerBinder.scan(new KindsH()), linear());
        assertEquals(3, r.bindings().size());
        assertTrue(r.unserved().isEmpty());
        Map<String, ActivityHandler> byStep = new LinkedHashMap<>();
        r.bindings().forEach(b -> byStep.put(b.step(), b.handler()));

        Step.begin(new Step.Info(1, "t", "i"));
        try {
            Object out = byStep.get("work").invoke(Map.of("a", 1L, "b", 2L));
            assertEquals(Map.of("a", 1L, "b", 2L, "done", true), out,
                    "the task wrapper reports the WHOLE return (it replaces server-side)");
            assertEquals(true, byStep.get("ok").invoke(Map.of()));
            assertNull(byStep.get("log").invoke(Map.of()), "an effect reports null (context untouched)");
        } finally {
            Step.end();
        }
    }

    @ForFlow("wf")
    static final class KindsH {
        public Map<String, Object> work(Map<String, Object> c) {
            Map<String, Object> n = new LinkedHashMap<>(c);
            n.put("done", true);
            return n;
        }
        public boolean ok(Map<String, Object> c) { return true; }
        public void log(Map<String, Object> c) { }
    }

    @Test @DisplayName("bind fails fast: non-boolean gate, boolean task, too many parameters")
    void bindRejectsKindMismatches() {
        assertThrows(IllegalStateException.class,
                () -> HandlerBinder.bind(HandlerBinder.scan(new BadGateH()), linear()));
        assertThrows(IllegalStateException.class,
                () -> HandlerBinder.bind(HandlerBinder.scan(new BoolTaskH()), linear()));
        assertThrows(IllegalStateException.class,
                () -> HandlerBinder.bind(HandlerBinder.scan(new TooManyParamsH()), linear()));
    }

    @ForFlow("wf")
    static final class BadGateH {
        public Map<String, Object> ok(Map<String, Object> c) { return c; }   // gate must return boolean
    }

    @ForFlow("wf")
    static final class BoolTaskH {
        public boolean work(Map<String, Object> c) { return true; }          // task must not return boolean
    }

    @ForFlow("wf")
    static final class TooManyParamsH {
        public Map<String, Object> work(Map<String, Object> a, Map<String, Object> b) { return a; }
    }

    @Test @DisplayName("bind reports unserved steps and applies queue defaulting")
    void unservedAndQueues() {
        WorkflowDefinition def = FlowSpec.define("wf", 1, Map.class, OneStep.class, (f, s) -> f
                .thenApply(s::served, "special-queue")
                .thenApply(s::someoneElses)).definition();
        HandlerBinder.Result r = HandlerBinder.bind(HandlerBinder.scan(new SubsetH()), def);
        assertEquals(1, r.bindings().size());
        assertEquals("special-queue", r.bindings().get(0).queue(), "explicit queue respected");
        assertEquals(List.of("someoneElses"), r.unserved());
    }

    @ForFlow("wf")
    static final class SubsetH {
        public Map<String, Object> served(Map<String, Object> c) { return c; }
    }


    @Test @DisplayName("a step takes only its input; the base is Step.base(), not a second parameter")
    void stepTakesOneParameter() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> HandlerBinder.bind(HandlerBinder.scan(new CtxParamH()), linear()));
        assertTrue(e.getMessage().contains("Step.base(Type.class)"), e.getMessage());
    }

    @ForFlow("wf")
    static final class CtxParamH {
        public Map<String, Object> work(Map<String, Object> base, String item) {
            return Map.of("v", base.get("rate"));
        }
    }

    private static WorkflowDefinition forked() {
        return FlowSpec.define("wf", 1, Map.class, ForkSteps.class, (f, s) ->
                Wiggle.allOf(f.thenApply(s::a1), f.thenApply(s::b1)).combine(s::merge)).definition();
    }

    @Test @DisplayName("fork combine: same-typed arms in fork order, ambient Step.base(), and a verbatim whole return")
    void forkCombine() throws Exception {
        HandlerBinder.Result r = HandlerBinder.bind(HandlerBinder.scan(new ForkCombineH()), forked());
        ActivityHandler merge = r.bindings().stream()
                .filter(b -> b.step().equals("merge")).findFirst().orElseThrow().handler();

        Step.begin(new Step.Info(1, "t", "i"));
        try {
            // staged under the reserved arm keys, as the engine stages them -- a bare arm name would
            // collide with a context key of the same name and be stripped along with it
            Object out = merge.invoke(Map.of("pre", "P",
                    "__arm__a1", Map.of("x", 1L), "__arm__b1", Map.of("y", 2L)));
            assertEquals(Map.of("pre", "P", "x", 1L, "y", 2L), out,
                    "combine reads its arms in fork order and the base ambiently, returns verbatim");
        } finally {
            Step.end();
        }
    }

    interface ForkSteps {
        Map<String, Object> a1(Map<String, Object> c);
        Map<String, Object> b1(Map<String, Object> c);
        Map<String, Object> merge(Map<String, Object> a, Map<String, Object> b);
    }

    interface EachSteps {
        String norm(String item);
        Map<String, Object> collect(Map<String, Object> base, List<String> items);
    }

    @ForFlow("wf")
    static final class ForkCombineH {
        public Map<String, Object> a1(Map<String, Object> c) { return c; }
        public Map<String, Object> b1(Map<String, Object> c) { return c; }
        public Map<String, Object> merge(Map<String, Object> a, Map<String, Object> b) {
            Map<String, Object> out = new LinkedHashMap<>(Step.base());   // ambient style: no base parameter
            out.putAll(a);
            out.putAll(b);
            return out;
        }
    }

    @Test @DisplayName("same-typed arms go to same-typed parameters in fork order; the base is Step.base()")
    void sameTypedArmsBindInForkOrder() throws Exception {
        HandlerBinder.Result r = HandlerBinder.bind(HandlerBinder.scan(new PositionalCombineH()), forked());
        ActivityHandler merge = r.bindings().stream()
                .filter(b -> b.step().equals("merge")).findFirst().orElseThrow().handler();

        Step.begin(new Step.Info(1, "t", "i"));
        try {
            // staged under the reserved arm keys, as the engine stages them -- a bare arm name would
            // collide with a context key of the same name and be stripped along with it
            Object out = merge.invoke(Map.of("pre", "P",
                    "__arm__a1", Map.of("x", 1L), "__arm__b1", Map.of("y", 2L)));
            assertEquals(Map.of("base", "P", "first", Map.of("x", 1L), "second", Map.of("y", 2L)), out,
                    "every arm is a Map, so the arms take the two Map parameters in fork order");
        } finally {
            Step.end();
        }
    }

    @ForFlow("wf")
    static final class PositionalCombineH {
        public Map<String, Object> a1(Map<String, Object> c) { return c; }
        public Map<String, Object> b1(Map<String, Object> c) { return c; }
        public Map<String, Object> merge(Map<String, Object> a, Map<String, Object> b) {
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("base", Step.base().get("pre"));
            out.put("first", a);
            out.put("second", b);
            return out;
        }
    }

    record Paid(long amount) {}

    record Shipped(String tracking) {}

    @Test @DisplayName("a combine's parameters are found by type, in any order, and need not take every arm")
    void forkCombineBindsByType() throws Exception {
        ActivityHandler merge = HandlerBinder.bind(HandlerBinder.scan(new TypedCombineH()), forked()).bindings()
                .stream().filter(b -> b.step().equals("merge")).findFirst().orElseThrow().handler();
        ActivityHandler shipOnly = HandlerBinder.bind(HandlerBinder.scan(new OneArmCombineH()), forked()).bindings()
                .stream().filter(b -> b.step().equals("merge")).findFirst().orElseThrow().handler();
        Map<String, Object> staged = Map.of("pre", "P",
                "__arm__a1", Map.of("amount", 5L), "__arm__b1", Map.of("tracking", "T1"));

        Step.begin(new Step.Info(1, "t", "i"));
        try {
            assertEquals(Map.of("tracking", "T1", "pre", "P", "amount", 5L), merge.invoke(staged));
            assertEquals(Map.of("tracking", "T1"), shipOnly.invoke(staged));
        } finally {
            Step.end();
        }
    }

    @ForFlow("wf")
    static final class TypedCombineH {
        public Paid a1(Map<String, Object> c) { return new Paid(5); }
        public Shipped b1(Map<String, Object> c) { return new Shipped("T1"); }
        public Map<String, Object> merge(Shipped s, Map<String, Object> base, Paid p) {
            return Map.of("tracking", s.tracking(), "pre", base.get("pre"), "amount", p.amount());
        }
    }

    @ForFlow("wf")
    static final class OneArmCombineH {
        public Paid a1(Map<String, Object> c) { return new Paid(5); }
        public Shipped b1(Map<String, Object> c) { return new Shipped("T1"); }
        public Map<String, Object> merge(Shipped s) { return Map.of("tracking", s.tracking()); }
    }

    @Test @DisplayName("two parameters that no arm matches would both take the base, and are refused")
    void forkCombineRefusesTwoBases() {
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> HandlerBinder.bind(HandlerBinder.scan(new TwoBasesH()), forked()));
        assertTrue(ex.getMessage().contains("both would take the pre-fork context"), ex.getMessage());
        assertTrue(ex.getMessage().contains("a1: Paid, b1: Shipped"), "names what the arms produce: " + ex.getMessage());
    }

    @ForFlow("wf")
    static final class TwoBasesH {
        public Paid a1(Map<String, Object> c) { return new Paid(5); }
        public Shipped b1(Map<String, Object> c) { return new Shipped("T1"); }
        public Map<String, Object> merge(Map<String, Object> a, Map<String, Object> b) { return a; }
    }

    @Test @DisplayName("a parameter left over after its type's arms are taken is refused, not given the base")
    void forkCombineRefusesLeftOverSameType() {
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> HandlerBinder.bind(HandlerBinder.scan(new LeftOverH()), forked()));
        assertTrue(ex.getMessage().contains("parameter 1 (Paid) is left over"), ex.getMessage());
        assertTrue(ex.getMessage().contains("a1: Paid, b1: Paid"), "names what the arms produce: " + ex.getMessage());
    }

    @ForFlow("wf")
    static final class LeftOverH {
        public Paid a1(Map<String, Object> c) { return new Paid(5); }
        public Paid b1(Map<String, Object> c) { return new Paid(7); }
        public Paid merge(Paid base, Paid a, Paid b) { return a; }
    }

    @Test @DisplayName("without every arm's handler, a combine takes the arms in fork order")
    void forkCombineFallsBackToForkOrder() throws Exception {
        ActivityHandler merge = HandlerBinder.bind(HandlerBinder.scan(new CombineOnlyH()), forked()).bindings()
                .stream().filter(b -> b.step().equals("merge")).findFirst().orElseThrow().handler();
        Step.begin(new Step.Info(1, "t", "i"));
        try {
            assertEquals(Map.of("first", Map.of("x", 1L), "second", Map.of("y", 2L)),
                    merge.invoke(Map.of("__arm__a1", Map.of("x", 1L), "__arm__b1", Map.of("y", 2L))));
        } finally {
            Step.end();
        }
    }

    @ForFlow("wf")
    static final class CombineOnlyH {
        public Map<String, Object> merge(Map<String, Object> a, Map<String, Object> b) {
            return Map.of("first", a, "second", b);
        }
    }

    private static WorkflowDefinition eachGraph() {
        return FlowSpec.define("wf", 1, Map.class, EachSteps.class, (f, s) ->
                f.thenForEach("per-item", "items", String.class, b -> b.thenApply(s::norm))
                        .combine(s::collect)).definition();
    }

    @Test @DisplayName("forEach combine: List keeps order, Set dedupes, Map is keyed like the input")
    void forEachCombineCollections() throws Exception {
        HandlerBinder.Result r = HandlerBinder.bind(HandlerBinder.scan(new EachCombineH()), eachGraph());
        ActivityHandler collect = r.bindings().stream()
                .filter(b -> b.step().equals("collect")).findFirst().orElseThrow().handler();

        Step.begin(new Step.Info(1, "t", "i"));
        try {
            // staged: base + the collected results under the forEach's reserved scratch key.
            // Reserved, not the bare node name: the name defaults to the collection key, so a bare
            // key would overwrite the very collection it fanned over. DynamicConstructsTest pins
            // the format; this test only has to stage what the engine would.
            Object out = collect.invoke(Map.of("pre", "P", "__forEach__per-item", List.of("x", "x", "y")));
            assertEquals(Map.of("pre", "P", "ordered", List.of("x", "x", "y"), "distinct", 2L), out);
        } finally {
            Step.end();
        }
    }

    @ForFlow("wf")
    static final class EachCombineH {
        public String norm(String item) { return item; }
        public Map<String, Object> collect(Map<String, Object> base, List<String> items) {
            Map<String, Object> out = new LinkedHashMap<>(base);
            out.put("ordered", items);
            out.put("distinct", (long) Set.copyOf(items).size());
            return out;
        }
    }

    @Test @DisplayName("forEach combine: two parameters that are not collections would both take the base")
    void forEachCombineRefusesTwoBases() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> HandlerBinder.bind(HandlerBinder.scan(new NoCollectionH()), eachGraph()));
        assertTrue(e.getMessage().contains("both would take the context"), e.getMessage());
    }

    @ForFlow("wf")
    static final class NoCollectionH {
        public String norm(String item) { return item; }
        public Map<String, Object> collect(Map<String, Object> base, String notACollection) { return base; }
    }
}
