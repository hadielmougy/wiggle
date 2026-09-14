package com.wiggle.client.worker;

import com.wiggle.client.worker.ActivityHandler;
import com.wiggle.client.flow.Wiggle;
import com.wiggle.core.WorkflowDefinition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    // ------------------------------------------------------------------ scan

    @Test @DisplayName("scan rejects an object without @Handlers, and a blank workflow name")
    void scanRejectsUnannotated() {
        assertThrows(IllegalArgumentException.class, () -> HandlerBinder.scan(new Object()));
        assertThrows(IllegalArgumentException.class, () -> HandlerBinder.scan(new BlankH()));
    }

    @Handlers("")
    static final class BlankH {
        public Map<String, Object> a(Map<String, Object> c) { return c; }
    }

    @Test @DisplayName("scan rejects two methods whose names collide under case-folding")
    void scanRejectsCollisions() {
        IllegalArgumentException e =
                assertThrows(IllegalArgumentException.class, () -> HandlerBinder.scan(new CollidingH()));
        assertTrue(e.getMessage().contains("ambiguous"), e.getMessage());
    }

    @Handlers("wf")
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

    @Handlers("wf")
    static final class DecoderH {
        @Decode public Map<String, Object> load(Map<String, Object> raw) { return raw; }
        public Map<String, Object> work(Map<String, Object> c) { return c; }
        public String helper() { return "not a handler"; }
    }

    // ------------------------------------------------------------------ bind: kinds & signatures

    private static WorkflowDefinition linear() {
        return Wiggle.define("wf", Map.class, OneStep.class, (f, s) -> f
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

    @Handlers("wf")
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

    @Handlers("wf")
    static final class BadGateH {
        public Map<String, Object> ok(Map<String, Object> c) { return c; }   // gate must return boolean
    }

    @Handlers("wf")
    static final class BoolTaskH {
        public boolean work(Map<String, Object> c) { return true; }          // task must not return boolean
    }

    @Handlers("wf")
    static final class TooManyParamsH {
        public Map<String, Object> work(Map<String, Object> a, Map<String, Object> b) { return a; }
    }

    @Test @DisplayName("bind reports unserved steps and applies queue defaulting")
    void unservedAndQueues() {
        WorkflowDefinition def = Wiggle.define("wf", Map.class, OneStep.class, (f, s) -> f
                .thenApply(s::served, "special-queue")
                .thenApply(s::someoneElses)).definition();
        HandlerBinder.Result r = HandlerBinder.bind(HandlerBinder.scan(new SubsetH()), def);
        assertEquals(1, r.bindings().size());
        assertEquals("special-queue", r.bindings().get(0).queue(), "explicit queue respected");
        assertEquals(List.of("someoneElses"), r.unserved());
    }

    @Handlers("wf")
    static final class SubsetH {
        public Map<String, Object> served(Map<String, Object> c) { return c; }
    }

    // ------------------------------------------------------------------ @Context parameter

    @Test @DisplayName("a @Context parameter delivers Step.base(); outside a base scope it fails clearly")
    void contextParameter() throws Exception {
        HandlerBinder.Result r = HandlerBinder.bind(HandlerBinder.scan(new CtxParamH()), linear());
        ActivityHandler work = r.bindings().stream()
                .filter(b -> b.step().equals("work")).findFirst().orElseThrow().handler();

        // inside an item scope: base is delivered as the parameter
        Step.begin(new Step.Info(1, "t", "i", Map.of("rate", 2L), true, 0, null));
        try {
            assertEquals(Map.of("v", 2L), work.invoke("item"));
        } finally {
            Step.end();
        }

        // outside any base scope: a clear error, not an NPE
        Step.begin(new Step.Info(1, "t", "i"));
        try {
            IllegalStateException e = assertThrows(IllegalStateException.class, () -> work.invoke("item"));
            assertTrue(e.getMessage().contains("@Context"), e.getMessage());
        } finally {
            Step.end();
        }
    }

    @Handlers("wf")
    static final class CtxParamH {
        public Map<String, Object> work(@Context Map<String, Object> base, String item) {
            return Map.of("v", base.get("rate"));
        }
        public boolean ok(Map<String, Object> c) { return true; }
        public void log(Map<String, Object> c) { }
    }

    // ------------------------------------------------------------------ combines

    private static WorkflowDefinition forked() {
        return Wiggle.define("wf", Map.class, ForkSteps.class, (f, s) ->
                Wiggle.allOf(f.thenApply(s::a1), f.thenApply(s::b1)).combine(s::merge)).definition();
    }

    @Test @DisplayName("fork combine: arms by position, ambient Step.base(), and a verbatim whole return")
    void forkCombine() throws Exception {
        HandlerBinder.Result r = HandlerBinder.bind(HandlerBinder.scan(new ForkCombineH()), forked());
        ActivityHandler merge = r.bindings().stream()
                .filter(b -> b.step().equals("merge")).findFirst().orElseThrow().handler();

        Step.begin(new Step.Info(1, "t", "i"));
        try {
            // the staged context: pre-fork base + one key per arm, keyed by the arm's step
            Object out = merge.invoke(Map.of("pre", "P", "a1", Map.of("x", 1L), "b1", Map.of("y", 2L)));
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
        Map<String, Object> collect(@Context Map<String, Object> base, List<String> items);
    }

    @Handlers("wf")
    static final class ForkCombineH {
        public Map<String, Object> a1(Map<String, Object> c) { return c; }
        public Map<String, Object> b1(Map<String, Object> c) { return c; }
        public Map<String, Object> merge(Map<String, Object> a, Map<String, Object> b) {
            Map<String, Object> out = new LinkedHashMap<>(Step.base());   // ambient style: no @Context param
            out.putAll(a);
            out.putAll(b);
            return out;
        }
    }

    @Test @DisplayName("a combine takes the arms by position, in fork order")
    void forkCombineBindsByPosition() throws Exception {
        HandlerBinder.Result r = HandlerBinder.bind(HandlerBinder.scan(new PositionalCombineH()), forked());
        ActivityHandler merge = r.bindings().stream()
                .filter(b -> b.step().equals("merge")).findFirst().orElseThrow().handler();

        Step.begin(new Step.Info(1, "t", "i"));
        try {
            Object out = merge.invoke(Map.of("pre", "P", "a1", Map.of("x", 1L), "b1", Map.of("y", 2L)));
            assertEquals(Map.of("base", "P", "first", Map.of("x", 1L), "second", Map.of("y", 2L)), out,
                    "parameter order is fork order: arm 'a' first, arm 'b' second");
        } finally {
            Step.end();
        }
    }

    @Handlers("wf")
    static final class PositionalCombineH {
        public Map<String, Object> a1(Map<String, Object> c) { return c; }
        public Map<String, Object> b1(Map<String, Object> c) { return c; }
        // @Context still needs its annotation -- it is what distinguishes the base from an arm
        public Map<String, Object> merge(@Context Map<String, Object> base,
                                         Map<String, Object> a, Map<String, Object> b) {
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("base", base.get("pre"));
            out.put("first", a);
            out.put("second", b);
            return out;
        }
    }

    @Test @DisplayName("a combine must take every arm, since they bind by position")
    void forkCombineMustTakeEveryArm() {
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> HandlerBinder.bind(HandlerBinder.scan(new BadCombineH()), forked()));
        assertTrue(ex.getMessage().contains("[a1, b1], in that order"),
                "the error names the arms and their order: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("ignore it"),
                "and says what to do about an arm you do not need: " + ex.getMessage());
    }

    @Handlers("wf")
    static final class BadCombineH {
        public Map<String, Object> a1(Map<String, Object> c) { return c; }
        public Map<String, Object> b1(Map<String, Object> c) { return c; }
        public Map<String, Object> merge(Map<String, Object> notAnnotated) { return notAnnotated; }
    }

    @Test @DisplayName("a combine that takes more parameters than the fork has arms is rejected")
    void forkCombineRejectsTooManyArms() {
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> HandlerBinder.bind(HandlerBinder.scan(new WideCombineH()), forked()));
        assertTrue(ex.getMessage().contains("more arms than the fork has"), ex.getMessage());
    }

    @Handlers("wf")
    static final class WideCombineH {
        public Map<String, Object> a1(Map<String, Object> c) { return c; }
        public Map<String, Object> b1(Map<String, Object> c) { return c; }
        public Map<String, Object> merge(Map<String, Object> a, Map<String, Object> b,
                                         Map<String, Object> c) { return a; }
    }

    private static WorkflowDefinition eachGraph() {
        return Wiggle.define("wf", Map.class, EachSteps.class, (f, s) ->
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
            // staged: base + the collected results under the forEach's name ("per-item")
            Object out = collect.invoke(Map.of("pre", "P", "per-item", List.of("x", "x", "y")));
            assertEquals(Map.of("pre", "P", "ordered", List.of("x", "x", "y"), "distinct", 2L), out);
        } finally {
            Step.end();
        }
    }

    @Handlers("wf")
    static final class EachCombineH {
        public String norm(String item) { return item; }
        public Map<String, Object> collect(@Context Map<String, Object> base, List<String> items) {
            Map<String, Object> out = new LinkedHashMap<>(base);
            out.put("ordered", items);
            out.put("distinct", (long) Set.copyOf(items).size());
            return out;
        }
    }

    @Test @DisplayName("forEach combine requires a collection parameter")
    void forEachCombineNeedsCollection() throws Exception {
        HandlerBinder.Result r = HandlerBinder.bind(HandlerBinder.scan(new NoCollectionH()), eachGraph());
        ActivityHandler collect = r.bindings().stream()
                .filter(b -> b.step().equals("collect")).findFirst().orElseThrow().handler();
        Step.begin(new Step.Info(1, "t", "i"));
        try {
            IllegalStateException e = assertThrows(IllegalStateException.class,
                    () -> collect.invoke(Map.of("per-item", List.of())));
            assertTrue(e.getMessage().contains("collection parameter"), e.getMessage());
        } finally {
            Step.end();
        }
    }

    @Handlers("wf")
    static final class NoCollectionH {
        public String norm(String item) { return item; }
        public Map<String, Object> collect(@Context Map<String, Object> baseOnly) { return baseOnly; }
    }
}
