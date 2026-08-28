package dev.wiggle.tests;

import dev.wiggle.client.dsl.Blueprint;
import dev.wiggle.client.dsl.Workflow;
import dev.wiggle.client.WiggleClient;
import dev.wiggle.client.worker.Worker;
import dev.wiggle.core.ExecutionMode;
import dev.wiggle.core.Ids;
import dev.wiggle.core.InstanceView;
import dev.wiggle.core.Json;
import dev.wiggle.server.ServerConfig;
import dev.wiggle.server.WiggleServer;
import dev.wiggle.dist.WiggleStorageFactory;
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

    private InstanceView run(Blueprint<Map<String, Object>> bp, Map<String, Object> input, String jdbcUrl)
            throws Exception {
        try (WiggleServer server = new WiggleServer(config(jdbcUrl), new WiggleStorageFactory()).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker w = new Worker(client, "dyn-" + Ids.next("x")).register(bp)) {
            w.start();
            return client.awaitCompletion(client.start(bp, input), Duration.ofSeconds(20));
        }
    }

    // ------------------------------------------------------------------ doWhile

    private static Blueprint<Map<String, Object>> counterLoop(ExecutionMode mode, AtomicInteger bodyRuns) {
        return Workflow.define("dyn-loop")
                .execution(mode)
                .step("init", ctx -> put(ctx, "i", 0L))
                .doWhile("more", ctx -> (Long) ctx.get("i") < 5,
                        b -> b.step("work", ctx -> {
                            bodyRuns.incrementAndGet();
                            return put(ctx, "i", (Long) ctx.get("i") + 1);
                        }))
                .step("after", ctx -> put(ctx, "done", true))
                .build();
    }

    @Test @DisplayName("doWhile iterates until the condition fails, in every execution mode")
    void loopIterates() throws Exception {
        for (ExecutionMode mode : ExecutionMode.values()) {
            if (mode == ExecutionMode.DEFAULT) continue;
            AtomicInteger bodyRuns = new AtomicInteger();
            InstanceView v = run(counterLoop(mode, bodyRuns), Map.of(), null);
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
        Blueprint<Map<String, Object>> bp = Workflow.define("dyn-loop-once")
                .doWhile("never-again", ctx -> false,
                        b -> b.step("work", ctx -> {
                            bodyRuns.incrementAndGet();
                            return put(ctx, "ran", true);
                        }))
                .step("after", ctx -> put(ctx, "done", true))
                .build();
        InstanceView v = run(bp, Map.of(), null);
        assertEquals("COMPLETED", v.status());
        assertEquals(1, bodyRuns.get(), "do-while body runs once even when the condition is false");
        assertEquals(true, Json.asObject(v.context()).get("done"));
    }

    // ----------------------------------------------------------------- forkEach

    /** Two-step branch: the second step proves the item payload survives along the branch. */
    private static Blueprint<Map<String, Object>> fanOut(ExecutionMode mode) {
        return Workflow.define("dyn-fan")
                .execution(mode)
                .forkEach("per-item", "items", "item", b -> b
                        .step("upper", ctx -> put(ctx, "out" + ctx.get("itemIndex"),
                                String.valueOf(ctx.get("item")).toUpperCase()))
                        .step("measure", ctx -> put(ctx, "len" + ctx.get("itemIndex"),
                                (long) String.valueOf(ctx.get("item")).length())))
                .step("after", ctx -> put(ctx, "done", true))
                .build();
    }

    @Test @DisplayName("forkEach fans out one branch per list element and merges the results")
    void fanOutOverItems() throws Exception {
        for (ExecutionMode mode : new ExecutionMode[]{ExecutionMode.SERVER, ExecutionMode.LOCAL_SYNC}) {
            InstanceView v = run(fanOut(mode), Map.of("items", List.of("ab", "cde", "f")), null);
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
                run(fanOut(ExecutionMode.SERVER), Map.of("items", List.of()), null).context()).get("done"),
                "empty list");
        assertEquals(true, Json.asObject(
                run(fanOut(ExecutionMode.SERVER), Map.of(), null).context()).get("done"),
                "missing key");
    }

    @Test @DisplayName("a non-list at the items key fails the instance with a clear error")
    void nonListFails() throws Exception {
        InstanceView v = run(fanOut(ExecutionMode.SERVER), Map.of("items", "oops"), null);
        assertEquals("FAILED", v.status());
        assertTrue(v.error().contains("not a list"), v.error());
        assertTrue(v.error().contains("items"), "names the offending key");
    }

    @Test @DisplayName("forkEach round-trips through the JDBC store (payload column, graph columns)")
    void fanOutOnJdbc() throws Exception {
        String url = "jdbc:h2:mem:dyn-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
        InstanceView v = run(fanOut(ExecutionMode.SERVER), Map.of("items", List.of("x", "yz")), url);
        assertEquals("COMPLETED", v.status());
        Map<String, Object> ctx = Json.asObject(v.context());
        assertEquals("X", ctx.get("out0"));
        assertEquals("YZ", ctx.get("out1"));
        assertEquals(2L, ctx.get("len1"));
        assertFalse(ctx.containsKey("item"), "payload stayed branch-scoped on JDBC too");
    }
}
