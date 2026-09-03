package com.wiggle.tests;

import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.client.WiggleClient;
import com.wiggle.client.worker.Handlers;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The runtime-shaped constructs: {@code doWhile} (a graph cycle through a predicate) and
 * {@code forkEach} (fan-out whose width is a list in the context), across execution modes.
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

    // ----------------------------------------------------------------- forkEach

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

    /** Two-step branch: the second step proves the item payload survives along the branch. */
    private static Blueprint fanOut(ExecutionMode mode) {
        return Workflow.define("dyn-fan")
                .execution(mode)
                .forkEach("per-item", "items", "item", b -> b
                        .step("upper")
                        .step("measure"))
                .step("after")
                .build();
    }

    @Handlers("dyn-fan")
    static final class FanH {
        public Map<String, Object> upper(Map<String, Object> ctx) {
            return put(ctx, "out" + ctx.get("itemIndex"), String.valueOf(ctx.get("item")).toUpperCase());
        }
        public Map<String, Object> measure(Map<String, Object> ctx) {
            return put(ctx, "len" + ctx.get("itemIndex"), (long) String.valueOf(ctx.get("item")).length());
        }
        public Map<String, Object> after(Map<String, Object> ctx) { return put(ctx, "done", true); }
    }

    @Test @DisplayName("forkEach fans out one branch per list element and merges the results")
    void fanOutOverItems() throws Exception {
        for (ExecutionMode mode : new ExecutionMode[]{ExecutionMode.SERVER, ExecutionMode.LOCAL_SYNC}) {
            InstanceView v = run(fanOut(mode), new FanH(), Map.of("items", List.of("ab", "cde", "f")), null);
            assertEquals("COMPLETED", v.status(), mode + " status");
            Map<String, Object> ctx = Json.asObject(v.context());
            assertEquals("AB", ctx.get("out0"), mode + " out0");
            assertEquals("CDE", ctx.get("out1"), mode + " out1");
            assertEquals("F", ctx.get("out2"), mode + " out2");
            assertEquals(2L, ctx.get("len0"), mode + " payload survived to the branch's second step");
            assertEquals(3L, ctx.get("len1"), mode + " len1");
            assertEquals(true, ctx.get("done"), mode + " continuation ran after the join");
            assertNull(ctx.get("item"), mode + " the per-branch item never leaks into shared context");
            assertNull(ctx.get("itemIndex"), mode + " nor does the index");
        }
    }

    @Test @DisplayName("an empty or missing items list skips straight past the join")
    void emptyListSkips() throws Exception {
        assertEquals(true, Json.asObject(
                run(fanOut(ExecutionMode.SERVER), new FanH(), Map.of("items", List.of()), null).context()).get("done"),
                "empty list");
        assertEquals(true, Json.asObject(
                run(fanOut(ExecutionMode.SERVER), new FanH(), Map.of(), null).context()).get("done"),
                "missing key");
    }

    @Test @DisplayName("a non-list at the items key fails the instance with a clear error")
    void nonListFails() throws Exception {
        InstanceView v = run(fanOut(ExecutionMode.SERVER), new FanH(), Map.of("items", "oops"), null);
        assertEquals("FAILED", v.status());
        assertTrue(v.error().contains("not a list"), v.error());
        assertTrue(v.error().contains("items"), "names the offending key");
    }

    @Test @DisplayName("forkEach round-trips through the JDBC store (payload column, graph columns)")
    void fanOutOnJdbc() throws Exception {
        String url = "jdbc:h2:mem:dyn-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
        InstanceView v = run(fanOut(ExecutionMode.SERVER), new FanH(), Map.of("items", List.of("x", "yz")), url);
        assertEquals("COMPLETED", v.status());
        Map<String, Object> ctx = Json.asObject(v.context());
        assertEquals("X", ctx.get("out0"));
        assertEquals("YZ", ctx.get("out1"));
        assertEquals(2L, ctx.get("len1"));
        assertFalse(ctx.containsKey("item"), "payload stayed branch-scoped on JDBC too");
    }
}
