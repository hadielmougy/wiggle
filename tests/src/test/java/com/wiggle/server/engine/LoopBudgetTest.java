package com.wiggle.server.engine;

import com.wiggle.tests.TestPorts;
import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.ForFlow;
import com.wiggle.client.worker.Worker;
import com.wiggle.core.ExecutionMode;
import com.wiggle.core.InstanceView;
import com.wiggle.core.Node;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * doWhile iteration budgets: a runaway loop (a guard that never goes false) is cut off at its
 * budget and FAILS the instance with a clear error, instead of hot-spinning workers and the
 * database forever — under SERVER dispatch and under local chaining alike. A loop that finishes
 * within its budget is untouched, and the bookkeeping counter never leaks into handler contexts
 * or the final context.
 */
class LoopBudgetTest {

    /** The steps this spec names; a worker binds them by name. */
    interface LoopSteps {
        boolean forever(Map<String, Object> c);
        boolean fewMore(Map<String, Object> c);
        Map<String, Object> spin(Map<String, Object> c);
        Map<String, Object> after(Map<String, Object> c);
    }

    interface OneStep {
        Map<String, Object> a(Map<String, Object> ctx);
        Map<String, Object> b(Map<String, Object> ctx);
        boolean g(Map<String, Object> ctx);
    }

    private static ServerConfig config() {
        return new ServerConfig(TestPorts.free(), "loop-test", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(30),
                Duration.ofMillis(200), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    @ForFlow("loop-wf")
    public static final class LoopHandlers {
        public boolean forever(Map<String, Object> ctx) { return true; }          // the bug
        public boolean fewMore(Map<String, Object> ctx) {
            return ((Number) ctx.getOrDefault("n", 0L)).longValue() < 3;
        }
        public Map<String, Object> spin(Map<String, Object> ctx) {
            Map<String, Object> next = new LinkedHashMap<>(ctx);
            next.put("n", ((Number) ctx.getOrDefault("n", 0L)).longValue() + 1);
            assertFalse(ctx.containsKey("__loops__"), "bookkeeping must not leak into handler ctx");
            return next;
        }
        public Map<String, Object> after(Map<String, Object> ctx) { return ctx; }
    }

    private static InstanceView run(FlowSpec bp, Duration timeout) throws Exception {
        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker worker = new Worker(client, "loop-w").registerHandler(new LoopHandlers())) {
            client.register(bp);
            worker.start();
            String id = client.start(bp, Map.of());
            return client.awaitCompletion(id, timeout);
        }
    }

    @Test @Timeout(30)
    @DisplayName("a runaway loop fails at its explicit budget (SERVER dispatch)")
    void runawayServerMode() throws Exception {
        FlowSpec bp = FlowSpec.define("loop-wf", Map.class, LoopSteps.class, (f, s) -> f
                .repeatWhile(s::forever, 7, b -> b.thenApply(s::spin))
                .thenApply(s::after));
        InstanceView v = run(bp, Duration.ofSeconds(20));
        assertEquals("FAILED", v.status());
        assertNotNull(v.error());
        assertTrue(v.error().contains("exceeded its budget of 7"), v.error());
        assertTrue(v.error().contains("forever"), v.error());
    }

    @Test @Timeout(30)
    @DisplayName("a runaway loop fails at its budget under local chaining too")
    void runawayLocalAsync() throws Exception {
        FlowSpec bp = FlowSpec.define("loop-wf", Map.class, LoopSteps.class, (f, s) -> f
                .repeatWhile(s::forever, 7, b -> b.thenApply(s::spin))
                .thenApply(s::after)
                .execution(ExecutionMode.LOCAL_ASYNC));
        InstanceView v = run(bp, Duration.ofSeconds(20));
        assertEquals("FAILED", v.status());
        assertTrue(v.error().contains("exceeded its budget"), v.error());
    }

    @Test @Timeout(30)
    @DisplayName("a loop that finishes within budget completes; the counter never reaches the context")
    void legitLoopUnaffected() throws Exception {
        FlowSpec bp = FlowSpec.define("loop-wf", Map.class, LoopSteps.class, (f, s) -> f
                .repeatWhile(s::fewMore, 10, b -> b.thenApply(s::spin))
                .thenApply(s::after));
        InstanceView v = run(bp, Duration.ofSeconds(20));
        assertEquals("COMPLETED", v.status());
        @SuppressWarnings("unchecked")
        Map<String, Object> ctx = (Map<String, Object>) v.context();
        assertEquals(3L, ((Number) ctx.get("n")).longValue());
        assertFalse(ctx.containsKey("__loops__"), "bookkeeping must not leak into the final context");
    }

    @Test
    @DisplayName("non-loop graphs serialize without the budget field — content hashes are stable")
    void hashStability() {
        var def = FlowSpec.define("plain", Map.class, OneStep.class, (f, s) -> f
                .thenApply(s::a)
                .thenFilter(s::g)
                .thenApply(s::b)).definition();
        for (Node n : def.nodes().values()) {
            assertFalse(n.toJson().containsKey("loopBudget"),
                    "non-loop node '" + n.name() + "' must not serialize a loopBudget");
        }
        var loop = FlowSpec.define("looped", Map.class, OneStep.class, (f, s) ->
                f.repeatWhile(s::g, 5, b -> b.thenApply(s::a))).definition();
        assertTrue(loop.nodes().values().stream().anyMatch(n -> n.toJson().containsKey("loopBudget")),
                "the loop guard serializes its budget");
    }
}
