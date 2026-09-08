package com.wiggle.tests;

import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.client.WiggleClient;
import com.wiggle.client.worker.Context;
import com.wiggle.client.worker.Handlers;
import com.wiggle.client.worker.Step;
import com.wiggle.client.worker.Worker;
import com.wiggle.core.ExecutionMode;
import com.wiggle.core.Ids;
import com.wiggle.core.InstanceView;
import com.wiggle.core.Json;
import com.wiggle.core.Node;
import com.wiggle.core.NodeKind;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import com.wiggle.dist.WiggleStorageFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The runtime-shaped constructs: {@code doWhile} (a graph cycle through a predicate) and
 * {@code forEach} (fan-out whose width is a collection in the context), across execution modes.
 *
 * <p>forEach semantics under test: <b>the element IS the item's context</b> — body handlers take
 * the item's current value and their return replaces it; the frozen base rides on
 * {@link Step#base()}; the mandatory combine receives the collected final values.
 */
class DynamicConstructsTest {

    private static Map<String, Object> put(Map<String, Object> ctx, String k, Object v) {
        Map<String, Object> n = new LinkedHashMap<>(ctx);
        n.put(k, v);
        return n;
    }

    private static ServerConfig config(String jdbcUrl) {
        return new ServerConfig(0, "dyn-node", jdbcUrl, jdbcUrl == null ? null : "sa",
                jdbcUrl == null ? null : "", 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    private InstanceView run(Blueprint bp, Object handlers, Map<String, Object> input, String jdbcUrl)
            throws Exception {
        try (WiggleServer server = new WiggleServer(config(jdbcUrl), new WiggleStorageFactory()).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker w = new Worker(client, "dyn-" + Ids.next("x")).register(bp).handlers(handlers)) {
            w.start();
            return client.awaitCompletion(client.start(bp, input), Duration.ofSeconds(20));
        }
    }

    // ------------------------------------------------------------------ doWhile

    private static Blueprint counterLoop(ExecutionMode mode) {
        return Workflow.define("dyn-loop")
                .execution(mode)
                .step("init")
                .doWhile("more", b -> b.step("work"))
                .step("after")
                .build();
    }

    @Handlers("dyn-loop")
    static final class LoopH {
        final AtomicInteger bodyRuns;
        LoopH(AtomicInteger bodyRuns) { this.bodyRuns = bodyRuns; }
        public Map<String, Object> init(Map<String, Object> ctx) { return put(ctx, "i", 0L); }
        public boolean more(Map<String, Object> ctx) { return (Long) ctx.get("i") < 5; }
        public Map<String, Object> work(Map<String, Object> ctx) {
            bodyRuns.incrementAndGet();
            return put(ctx, "i", (Long) ctx.get("i") + 1);
        }
        public Map<String, Object> after(Map<String, Object> ctx) { return put(ctx, "done", true); }
    }

    @Test @DisplayName("doWhile iterates until the condition fails, in every execution mode")
    void loopIterates() throws Exception {
        for (ExecutionMode mode : ExecutionMode.values()) {
            if (mode == ExecutionMode.DEFAULT) continue;
            AtomicInteger bodyRuns = new AtomicInteger();
            InstanceView v = run(counterLoop(mode), new LoopH(bodyRuns), Map.of(), null);
            assertEquals("COMPLETED", v.status(), mode + " status");
            Map<String, Object> ctx = Json.asObject(v.context());
            assertEquals(5L, ctx.get("i"), mode + " loop counter");
            assertEquals(true, ctx.get("done"), mode + " continuation ran");
            assertEquals(5, bodyRuns.get(), mode + " body ran exactly five times");
        }
    }

    @Test @DisplayName("doWhile runs its body at least once")
    void loopRunsAtLeastOnce() throws Exception {
        AtomicInteger bodyRuns = new AtomicInteger();
        Blueprint bp = Workflow.define("dyn-loop-once")
                .doWhile("never-again", b -> b.step("work"))
                .step("after")
                .build();
        InstanceView v = run(bp, new LoopOnceH(bodyRuns), Map.of(), null);
        assertEquals("COMPLETED", v.status());
        assertEquals(1, bodyRuns.get(), "do-while body runs once even when the condition is false");
        assertEquals(true, Json.asObject(v.context()).get("done"));
    }

    @Handlers("dyn-loop-once")
    static final class LoopOnceH {
        final AtomicInteger bodyRuns;
        LoopOnceH(AtomicInteger bodyRuns) { this.bodyRuns = bodyRuns; }
        public boolean neverAgain(Map<String, Object> ctx) { return false; }
        public Map<String, Object> work(Map<String, Object> ctx) {
            bodyRuns.incrementAndGet();
            return put(ctx, "ran", true);
        }
        public Map<String, Object> after(Map<String, Object> ctx) { return put(ctx, "done", true); }
    }

    // ----------------------------------------------------------------- forEach

    /** Two-step body: the item value evolves scalar -> map, proving the value threads the body. */
    private static Blueprint fanOut(ExecutionMode mode) {
        return Workflow.define("dyn-fan")
                .execution(mode)
                .forEach("per-item", "items", b -> b
                        .step("upper")
                        .step("measure"))
                .combine("collect")
                .step("after")
                .build();
    }

    @Handlers("dyn-fan")
    static final class FanH {
        /** The handler's parameter IS the element; base data and the index come from Step. */
        public Map<String, Object> upper(String item) {
            Map<String, Object> v = new LinkedHashMap<>();
            v.put("out", item.toUpperCase());
            v.put("idx", Step.itemIndex());                       // ambient item position
            v.put("tag", Step.base().get("tag"));                 // ambient read-only base
            return v;
        }
        public Map<String, Object> measure(Map<String, Object> v) {
            return put(v, "len", (long) String.valueOf(v.get("out")).length());
        }
        public Map<String, Object> collect(@Context Map<String, Object> base,
                                           List<Map<String, Object>> items) {
            Map<String, Object> out = new LinkedHashMap<>(base);
            for (Map<String, Object> item : items) {
                long i = ((Number) item.get("idx")).longValue();
                out.put("out" + i, item.get("out"));
                out.put("len" + i, item.get("len"));
                out.put("tag" + i, item.get("tag"));
            }
            return out;
        }
        public Map<String, Object> after(Map<String, Object> ctx) { return put(ctx, "done", true); }
    }

    @Test @DisplayName("forEach: the element is the item's context; base and index ride on Step")
    void fanOutOverItems() throws Exception {
        for (ExecutionMode mode : new ExecutionMode[]{ExecutionMode.SERVER, ExecutionMode.LOCAL_SYNC}) {
            InstanceView v = run(fanOut(mode), new FanH(),
                    new LinkedHashMap<>(Map.of("items", List.of("ab", "cde", "f"), "tag", "T")), null);
            assertEquals("COMPLETED", v.status(), mode + " status");
            Map<String, Object> ctx = Json.asObject(v.context());
            assertEquals("AB", ctx.get("out0"), mode + " out0");
            assertEquals("CDE", ctx.get("out1"), mode + " out1");
            assertEquals("F", ctx.get("out2"), mode + " out2");
            assertEquals(2L, ctx.get("len0"), mode + " the item value threads to the body's second step");
            assertEquals("T", ctx.get("tag1"), mode + " Step.base() delivered the frozen base");
            assertEquals(true, ctx.get("done"), mode + " continuation ran after the combine");
            assertEquals(List.of("ab", "cde", "f"), ctx.get("items"),
                    mode + " the input collection survives untouched in the shared context");
            assertFalse(ctx.containsKey("per-item"), mode + " the collected-results scratch key is stripped");
        }
    }

    @Test @DisplayName("default-name shorthand: forEach(itemsKey, body) names the node after the collection")
    void shorthandDefaultsNameToItemsKey() {
        Blueprint bp = Workflow.define("dyn-fan-short")
                .forEach("items", b -> b.step("upper"))
                .combine("collect")
                .build();
        Node dyn = bp.definition().nodes().values().stream()
                .filter(n -> n.kind() == NodeKind.DYN_FORK)
                .findFirst().orElseThrow(() -> new AssertionError("no DYN_FORK node"));
        assertEquals("items", dyn.name(), "node name defaults to the collection key");
        assertEquals("items", dyn.itemsKey(), "collection key");
    }

    @Test @DisplayName("a map input fans out per entry; the combine receives a map keyed like the input")
    void mapInputCollectsAsMap() throws Exception {
        Blueprint bp = Workflow.define("dyn-fan-map")
                .forEach("per-entry", "prices", b -> b.step("tag"))
                .combine("collect")
                .step("after")
                .build();
        InstanceView v = run(bp, new MapFanH(),
                new LinkedHashMap<>(Map.of("prices", new LinkedHashMap<>(Map.of("eu", 10L, "us", 12L)))), null);
        assertEquals("COMPLETED", v.status());
        Map<String, Object> ctx = Json.asObject(v.context());
        assertEquals("eu:10", ctx.get("tagged-eu"), "map entry keyed result");
        assertEquals("us:12", ctx.get("tagged-us"));
        assertEquals(true, ctx.get("done"));
    }

    @Handlers("dyn-fan-map")
    static final class MapFanH {
        /** Two-param style: the frozen base as a @Context parameter instead of Step.base(). */
        public String tag(@Context Map<String, Object> base, Long value) {
            boolean sawBase = base.containsKey("prices");          // the full pre-forEach context
            return Step.itemMapKey() + ":" + value + (sawBase ? "" : ":no-base");
        }
        public Map<String, Object> collect(@Context Map<String, Object> base,
                                           Map<String, String> results) {
            Map<String, Object> out = new LinkedHashMap<>(base);
            results.forEach((k, tagged) -> out.put("tagged-" + k, tagged));
            return out;
        }
        public Map<String, Object> after(Map<String, Object> ctx) { return put(ctx, "done", true); }
    }

    @Test @DisplayName("scalar items flow scalar-to-scalar; a Set combine parameter deduplicates")
    void setParamDeduplicates() throws Exception {
        Blueprint bp = Workflow.define("dyn-fan-set")
                .forEach("per-item", "items", b -> b.step("norm"))
                .combine("collect")
                .build();
        InstanceView v = run(bp, new SetFanH(), Map.of("items", List.of("x", "x", "y")), null);
        assertEquals("COMPLETED", v.status());
        assertEquals(2L, Json.asObject(v.context()).get("distinct"), "duplicates collapse in a Set");
    }

    @Handlers("dyn-fan-set")
    static final class SetFanH {
        public String norm(String item) { return item.toUpperCase(); }   // scalar in, scalar out
        /** Ambient style: no @Context parameter — the base comes from Step.base() instead. */
        public Map<String, Object> collect(Set<String> results) {
            return put(Step.base(), "distinct", (long) results.size());
        }
    }

    @Test @DisplayName("an empty or missing collection skips the body AND the combine")
    void emptyListSkips() throws Exception {
        assertEquals(true, Json.asObject(
                run(fanOut(ExecutionMode.SERVER), new FanH(),
                        new LinkedHashMap<>(Map.of("items", List.of(), "tag", "T")), null)
                        .context()).get("done"),
                "empty list");
        assertEquals(true, Json.asObject(
                run(fanOut(ExecutionMode.SERVER), new FanH(),
                        new LinkedHashMap<>(Map.of("tag", "T")), null).context()).get("done"),
                "missing key");
    }

    @Test @DisplayName("a scalar at the items key fails the instance with a clear error")
    void nonListFails() throws Exception {
        InstanceView v = run(fanOut(ExecutionMode.SERVER), new FanH(),
                new LinkedHashMap<>(Map.of("items", "oops", "tag", "T")), null);
        assertEquals("FAILED", v.status());
        assertTrue(v.error().contains("not a list or map"), v.error());
        assertTrue(v.error().contains("items"), "names the offending key");
    }

    @Test @DisplayName("forEach round-trips through the JDBC store (payload column, graph columns)")
    void fanOutOnJdbc() throws Exception {
        String url = "jdbc:h2:mem:dyn-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
        InstanceView v = run(fanOut(ExecutionMode.SERVER), new FanH(),
                new LinkedHashMap<>(Map.of("items", List.of("x", "yz"), "tag", "T")), url);
        assertEquals("COMPLETED", v.status());
        Map<String, Object> ctx = Json.asObject(v.context());
        assertEquals("X", ctx.get("out0"));
        assertEquals("YZ", ctx.get("out1"));
        assertEquals(2L, ctx.get("len1"));
        assertFalse(ctx.containsKey("per-item"), "scratch stayed out of the shared context on JDBC too");
    }
}
