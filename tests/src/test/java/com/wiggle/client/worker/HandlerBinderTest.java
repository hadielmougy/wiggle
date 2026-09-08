package com.wiggle.client.worker;

import com.wiggle.client.dsl.ActivityHandler;
import com.wiggle.client.dsl.Branch;
import com.wiggle.client.dsl.Workflow;
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
        return Workflow.define("wf").step("work").gate("ok").effect("log").build().definition();
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
        WorkflowDefinition def = Workflow.define("wf")
                .step("served", "special-queue")
                .step("someone-elses")
                .build().definition();
        HandlerBinder.Result r = HandlerBinder.bind(HandlerBinder.scan(new SubsetH()), def);
        assertEquals(1, r.bindings().size());
        assertEquals("special-queue", r.bindings().get(0).queue(), "explicit queue respected");
        assertEquals(List.of("someone-elses"), r.unserved());
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
        return Workflow.define("wf")
                .fork(Branch.of("a", s -> s.step("a1")),
                      Branch.of("b", s -> s.step("b1")))
                .combine("merge")
                .build().definition();
    }

    @Test @DisplayName("fork combine: @Arm binding, ambient Step.base(), and a verbatim whole return")
    void forkCombine() throws Exception {
        HandlerBinder.Result r = HandlerBinder.bind(HandlerBinder.scan(new ForkCombineH()), forked());
        ActivityHandler merge = r.bindings().stream()
                .filter(b -> b.step().equals("merge")).findFirst().orElseThrow().handler();

        Step.begin(new Step.Info(1, "t", "i"));
        try {
            // the staged context: pre-fork base + one key per arm
            Object out = merge.invoke(Map.of("pre", "P", "a", Map.of("x", 1L), "b", Map.of("y", 2L)));
            assertEquals(Map.of("pre", "P", "x", 1L, "y", 2L), out,
                    "combine reads arms as @Arm params and the base ambiently, returns verbatim");
        } finally {
            Step.end();
        }
    }

    @Handlers("wf")
    static final class ForkCombineH {
        public Map<String, Object> a1(Map<String, Object> c) { return c; }
        public Map<String, Object> b1(Map<String, Object> c) { return c; }
        public Map<String, Object> merge(@Arm("a") Map<String, Object> a, @Arm("b") Map<String, Object> b) {
            Map<String, Object> out = new LinkedHashMap<>(Step.base());   // ambient style: no @Context param
            out.putAll(a);
            out.putAll(b);
            return out;
        }
    }

    @Test @DisplayName("fork combine rejects a plain (un-annotated) parameter")
    void forkCombineRejectsPlainParam() throws Exception {
        HandlerBinder.Result r = HandlerBinder.bind(HandlerBinder.scan(new BadCombineH()), forked());
        ActivityHandler merge = r.bindings().stream()
                .filter(b -> b.step().equals("merge")).findFirst().orElseThrow().handler();
        Step.begin(new Step.Info(1, "t", "i"));
        try {
            assertThrows(IllegalStateException.class, () -> merge.invoke(Map.of()));
        } finally {
            Step.end();
        }
    }

    @Handlers("wf")
    static final class BadCombineH {
        public Map<String, Object> a1(Map<String, Object> c) { return c; }
        public Map<String, Object> b1(Map<String, Object> c) { return c; }
        public Map<String, Object> merge(Map<String, Object> notAnnotated) { return notAnnotated; }
    }

    private static WorkflowDefinition eachGraph() {
        return Workflow.define("wf")
                .forEach("per-item", "items", b -> b.step("norm"))
                .combine("collect")
                .build().definition();
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
