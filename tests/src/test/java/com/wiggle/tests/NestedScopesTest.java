package com.wiggle.tests;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.flow.Wiggle;
import com.wiggle.client.worker.Context;
import com.wiggle.client.worker.ForFlow;
import com.wiggle.client.worker.Worker;
import com.wiggle.core.ExecutionMode;
import com.wiggle.core.Ids;
import com.wiggle.core.InstanceView;
import com.wiggle.core.Json;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import com.wiggle.dist.WiggleStorageFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Scope NESTING: fork, forEach and doWhile composed inside each other, in every pairing. Each
 * construct pushes a scope frame onto its children and the join pops it, so what these tests pin
 * down is that a construct behaves identically at any depth: an inner combine replaces the
 * ENCLOSING scope's view (never the shared context), an inner forEach fans out over the enclosing
 * view (never the shared context), and item identity survives an inner join.
 */
class NestedScopesTest {

    private static ServerConfig config() {
        return new ServerConfig(TestPorts.free(), "nest-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    private InstanceView run(FlowSpec bp, Object handlers, Map<String, Object> input) throws Exception {
        try (WiggleServer server = new WiggleServer(config(), new WiggleStorageFactory()).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker w = new Worker(client, "nest-" + Ids.next("x")).registerHandler(handlers)) {
            client.register(bp);
            w.start();
            return client.awaitCompletion(client.start(bp, input), Duration.ofSeconds(20));
        }
    }

    // ---------- fork inside a forEach item (scalar items) ----------

    interface ForkInForEachSteps {
        Map<String, Object> up(String item);
        Map<String, Object> down(String item);
        Map<String, Object> merge(@Context String item, Map<String, Object> u, Map<String, Object> d);
        Map<String, Object> gather(@Context Map<String, Object> base, List<Map<String, Object>> results);
    }

    private static FlowSpec forkInForEach(ExecutionMode mode) {
        return FlowSpec.define("fork-in-foreach", Map.class, ForkInForEachSteps.class, (f, s) -> f
                .execution(mode)
                .thenForEach("per-item", "items", String.class, item ->
                        Wiggle.allOf(item.thenApply(s::up), item.thenApply(s::down))
                                .combineWithContext(s::merge))
                .combine(s::gather));
    }

    @ForFlow("fork-in-foreach")
    static final class ForkInForEachH {
        public Map<String, Object> up(String item) { return Map.of("u", item.toUpperCase()); }
        public Map<String, Object> down(String item) { return Map.of("d", item.toLowerCase()); }
        /** The inner combine: @Context is the pre-fork scope view — the ITEM itself, a scalar. */
        public Map<String, Object> merge(@Context String item, Map<String, Object> u, Map<String, Object> d) {
            return Map.of("v", item + ":" + u.get("u") + "+" + d.get("d"));
        }
        public Map<String, Object> gather(@Context Map<String, Object> base, List<Map<String, Object>> results) {
            Map<String, Object> out = new LinkedHashMap<>(base);
            out.put("collected", results.stream().map(r -> String.valueOf(r.get("v"))).toList());
            return out;
        }
    }

    @Test @DisplayName("fork inside a forEach item: arms fork off the item value, the inner combine becomes the item's result")
    void forkInsideForEach() throws Exception {
        for (ExecutionMode mode : new ExecutionMode[]{ExecutionMode.SERVER, ExecutionMode.LOCAL_SYNC}) {
            InstanceView v = run(forkInForEach(mode), new ForkInForEachH(),
                    new LinkedHashMap<>(Map.of("items", List.of("Ab", "Cd"), "tag", "T")));
            assertEquals("COMPLETED", v.status(), mode + " status");
            Map<String, Object> ctx = Json.asObject(v.context());
            assertEquals(List.of("Ab:AB+ab", "Cd:CD+cd"), ctx.get("collected"), mode + " collected");
            assertEquals("T", ctx.get("tag"), mode + " the outer combine's base survives");
            assertFalse(Json.write(ctx).contains("__"), mode + " no engine bookkeeping leaked: " + ctx);
        }
    }

    // ---------- forEach inside a fork arm ----------

    interface ForEachInForkSteps {
        Map<String, Object> seed(Map<String, Object> ctx);
        String bump(String item);
        Map<String, Object> innerFold(@Context Map<String, Object> base, List<String> results);
        Map<String, Object> plain(Map<String, Object> ctx);
        Map<String, Object> outerFold(Map<String, Object> wide, Map<String, Object> narrow);
    }

    private static FlowSpec forEachInFork() {
        return FlowSpec.define("foreach-in-fork", Map.class, ForEachInForkSteps.class, (f, s) -> {
            var seeded = f.thenApply(s::seed);
            var wide = seeded.thenForEach("inner", "items", String.class, i -> i.thenApply(s::bump))
                    .combine(s::innerFold);
            var narrow = seeded.thenApply(s::plain);
            return Wiggle.allOf(wide, narrow).combine(s::outerFold);
        });
    }

    @ForFlow("foreach-in-fork")
    static final class ForEachInForkH {
        public Map<String, Object> seed(Map<String, Object> ctx) {
            Map<String, Object> out = new LinkedHashMap<>(ctx);
            out.put("items", List.of("a", "b"));
            out.put("keep", "K");
            return out;
        }
        public String bump(String item) { return item + "!"; }
        /** The inner combine's base is the ARM's view (where the forEach ran), not the shared context. */
        public Map<String, Object> innerFold(@Context Map<String, Object> base, List<String> results) {
            return Map.of("inner", results, "keep", base.get("keep"));
        }
        public Map<String, Object> plain(Map<String, Object> ctx) { return Map.of("plainDone", true); }
        public Map<String, Object> outerFold(Map<String, Object> wide, Map<String, Object> narrow) {
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("inner", wide.get("inner"));
            out.put("keep", wide.get("keep"));
            out.put("plainDone", narrow.get("plainDone"));
            return out;
        }
    }

    @Test @DisplayName("forEach inside a fork arm: it fans out over the ARM's view and folds back into the arm")
    void forEachInsideFork() throws Exception {
        InstanceView v = run(forEachInFork(), new ForEachInForkH(), new LinkedHashMap<>());
        assertEquals("COMPLETED", v.status());
        Map<String, Object> ctx = Json.asObject(v.context());
        assertEquals(List.of("a!", "b!"), ctx.get("inner"));
        assertEquals("K", ctx.get("keep"), "the inner combine saw the arm view as its base");
        assertEquals(true, ctx.get("plainDone"));
        assertFalse(Json.write(ctx).contains("__"), "no engine bookkeeping leaked: " + ctx);
    }

    // ---------- fork inside a fork arm ----------

    interface ForkInForkSteps {
        Map<String, Object> seed(Map<String, Object> ctx);
        Map<String, Object> a1(Map<String, Object> ctx);
        Map<String, Object> a2(Map<String, Object> ctx);
        Map<String, Object> innerMerge(Map<String, Object> x, Map<String, Object> y);
        Map<String, Object> b1(Map<String, Object> ctx);
        Map<String, Object> outerMerge(Map<String, Object> a, Map<String, Object> b);
    }

    private static FlowSpec forkInFork() {
        return FlowSpec.define("fork-in-fork", Map.class, ForkInForkSteps.class, (f, s) -> {
            var seeded = f.thenApply(s::seed);
            var armA = Wiggle.allOf(seeded.thenApply(s::a1), seeded.thenApply(s::a2)).combine(s::innerMerge);
            var armB = seeded.thenApply(s::b1);
            return Wiggle.allOf(armA, armB).combine(s::outerMerge);
        });
    }

    @ForFlow("fork-in-fork")
    static final class ForkInForkH {
        public Map<String, Object> seed(Map<String, Object> ctx) { return Map.of("s", 1L); }
        public Map<String, Object> a1(Map<String, Object> ctx) { return Map.of("x", 1L); }
        public Map<String, Object> a2(Map<String, Object> ctx) { return Map.of("y", 2L); }
        /** The inner combine's return becomes the OUTER arm's view, invisible to arm B. */
        public Map<String, Object> innerMerge(Map<String, Object> x, Map<String, Object> y) {
            return Map.of("xy", x.get("x") + "." + y.get("y"));
        }
        public Map<String, Object> b1(Map<String, Object> ctx) { return Map.of("z", 3L); }
        public Map<String, Object> outerMerge(Map<String, Object> a, Map<String, Object> b) {
            return Map.of("xy", a.get("xy"), "z", b.get("z"));
        }
    }

    @Test @DisplayName("fork inside a fork arm: the inner combine replaces the outer arm's view, unseen by its sibling")
    void forkInsideFork() throws Exception {
        InstanceView v = run(forkInFork(), new ForkInForkH(), new LinkedHashMap<>());
        assertEquals("COMPLETED", v.status());
        Map<String, Object> ctx = Json.asObject(v.context());
        assertEquals("1.2", ctx.get("xy"));
        assertEquals(3L, ctx.get("z"));
        assertFalse(Json.write(ctx).contains("__"), "no engine bookkeeping leaked: " + ctx);
    }

    // ---------- forEach inside a forEach item ----------

    interface ForEachInForEachSteps {
        Long twice(Long n);
        Map<String, Object> innerSum(@Context Map<String, Object> base, List<Long> results);
        Map<String, Object> outerGather(List<Map<String, Object>> results);
    }

    private static FlowSpec forEachInForEach() {
        return FlowSpec.define("foreach-in-foreach", Map.class, ForEachInForEachSteps.class, (f, s) -> f
                .thenForEach("outer", "groups", Map.class, g ->
                        g.thenForEach("innerFe", "nums", Long.class, n -> n.thenApply(s::twice))
                                .combine(s::innerSum))
                .combine(s::outerGather));
    }

    @ForFlow("foreach-in-foreach")
    static final class ForEachInForEachH {
        public Long twice(Long n) { return n * 2; }
        /** The inner forEach fanned out over the ITEM's "nums", not a shared-context key. */
        public Map<String, Object> innerSum(@Context Map<String, Object> base, List<Long> results) {
            long sum = results.stream().mapToLong(Long::longValue).sum();
            return Map.of("sum", sum, "width", (long) Json.asArray(base.get("nums")).size());
        }
        public Map<String, Object> outerGather(List<Map<String, Object>> results) {
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("sums", results.stream().map(r -> r.get("sum")).toList());
            out.put("widths", results.stream().map(r -> r.get("width")).toList());
            return out;
        }
    }

    @Test @DisplayName("forEach inside a forEach item: the inner fan-out reads the ITEM's collection")
    void forEachInsideForEach() throws Exception {
        InstanceView v = run(forEachInForEach(), new ForEachInForEachH(), new LinkedHashMap<>(Map.of(
                "groups", List.of(Map.of("nums", List.of(1L, 2L)), Map.of("nums", List.of(5L))))));
        assertEquals("COMPLETED", v.status());
        Map<String, Object> ctx = Json.asObject(v.context());
        assertEquals(List.of(6L, 10L), ctx.get("sums"));
        assertEquals(List.of(2L, 1L), ctx.get("widths"), "each inner combine's base was its own item");
        assertFalse(Json.write(ctx).contains("__"), "no engine bookkeeping leaked: " + ctx);
    }

    // ---------- doWhile inside a forEach item ----------

    interface LoopInForEachSteps {
        Map<String, Object> inc(Map<String, Object> item);
        boolean more(Map<String, Object> item);
        Map<String, Object> counts(List<Map<String, Object>> results);
    }

    private static FlowSpec loopInForEach() {
        return FlowSpec.define("loop-in-foreach", Map.class, LoopInForEachSteps.class, (f, s) -> f
                .thenForEach("seeds", Map.class, item ->
                        item.repeatWhile(s::more, b -> b.thenApply(s::inc)))
                .combine(s::counts));
    }

    @ForFlow("loop-in-foreach")
    static final class LoopInForEachH {
        public Map<String, Object> inc(Map<String, Object> item) {
            Map<String, Object> out = new LinkedHashMap<>(item);
            out.put("n", ((Number) item.get("n")).longValue() + 1);
            return out;
        }
        public boolean more(Map<String, Object> item) {
            return ((Number) item.get("n")).longValue() < ((Number) item.get("limit")).longValue();
        }
        public Map<String, Object> counts(List<Map<String, Object>> results) {
            return Map.of("counts", results.stream().map(r -> r.get("n")).toList());
        }
    }

    @Test @DisplayName("doWhile inside a forEach item: each item loops on its own value to its own limit")
    void loopInsideForEach() throws Exception {
        InstanceView v = run(loopInForEach(), new LoopInForEachH(), new LinkedHashMap<>(Map.of(
                "seeds", List.of(
                        new LinkedHashMap<>(Map.of("n", 0L, "limit", 2L)),
                        new LinkedHashMap<>(Map.of("n", 0L, "limit", 3L))))));
        assertEquals("COMPLETED", v.status());
        Map<String, Object> ctx = Json.asObject(v.context());
        assertEquals(List.of(2L, 3L), ctx.get("counts"));
        assertFalse(Json.write(ctx).contains("__"), "no engine bookkeeping leaked: " + ctx);
    }
}
