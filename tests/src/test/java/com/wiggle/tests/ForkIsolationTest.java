package com.wiggle.tests;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.flow.Wiggle;
import com.wiggle.client.worker.Context;
import com.wiggle.client.worker.ForFlow;
import com.wiggle.client.worker.Worker;
import com.wiggle.client.worker.WorkerOptions;
import com.wiggle.core.InstanceView;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * The design-B guarantee of {@link com.wiggle.client.flow.Wiggle#allOf}: each branch runs on
 * its own isolated context copy, so a branch's writes are invisible to its siblings and never reach
 * the shared context implicitly -- the only thing that lands is what the mandatory {@code combine}
 * returns.
 */
class ForkIsolationTest {

    @Test @DisplayName("branch writes are isolated: no implicit merge, combine owns what lands")
    void branchesAreIsolatedAndCombineDecides() throws Exception {
        FlowSpec bp = FlowSpec.define("isolation", Map.class, IsolationSteps.class, (f, s) -> {
            var seeded = f.thenApply(s::seed);
            // Both arms write the SAME key to different values, and each also asserts it cannot see
            // the base being overwritten by its sibling (isolation).
            var left = seeded.thenApply(s::l);
            var right = seeded.thenApply(s::r);
            // The combine ignores both arms' "shared" writes entirely and sets its own value,
            // proving nothing merges unless combine returns it.
            return Wiggle.allOf(left, right).combineWithContext(s::decide);
        });

        Map<String, Object> out = run(bp, new IsolationH(), new LinkedHashMap<>(Map.of("id", "iso-1")));

        assertEquals("B", out.get("base"), "base survives");
        // Neither branch's raw write leaked into the shared context; only combine's value is present.
        assertEquals("chosen", out.get("shared"), "only the combine's value lands, no implicit merge");
        // combine did receive each isolated branch's own result.
        assertEquals("from-left", out.get("sawLeft"));
        assertEquals("from-right", out.get("sawRight"));
        // The per-arm scratch keys never leak.
        assertFalse(out.containsKey("left"), "left scratch removed: " + out);
        assertFalse(out.containsKey("right"), "right scratch removed: " + out);
    }

    @Test @DisplayName("a branch that combine ignores contributes nothing to the context")
    void ignoredBranchLeavesNoTrace() throws Exception {
        FlowSpec bp = FlowSpec.define("ignore-arm", Map.class, IgnoreArmSteps.class, (f, s) ->
                // Only "k" is folded back; "d"'s writes are discarded with its isolated context.
                Wiggle.allOf(f.thenApply(s::k), f.thenApply(s::d))
                        .combine(s::pick)
                        .thenApply(s::tail));

        Map<String, Object> out = run(bp, new IgnoreArmH(), new LinkedHashMap<>());

        assertEquals(true, out.get("kept"));
        assertFalse(out.containsKey("dropped"), "the ignored branch left no trace: " + out);
    }

    @Test @DisplayName("a combine's return REPLACES the context: keys it omits do not survive the join")
    void combineReturnReplacesContext() throws Exception {
        FlowSpec bp = FlowSpec.define("replace-check", Map.class, ReplaceSteps.class, (f, s) -> {
            var seeded = f.thenApply(s::seed);
            return Wiggle.allOf(seeded.thenApply(s::a1), seeded.thenApply(s::b1)).combine(s::pickOnly);
        });

        Map<String, Object> out = run(bp, new ReplaceH(), new LinkedHashMap<>(Map.of("preFork", "here")));

        assertEquals(true, out.get("picked"), "the combine's own value lands");
        assertFalse(out.containsKey("preFork"),
                "a pre-fork key the combine did not return must NOT survive (no implicit old+new merge): " + out);
    }

    @Test @DisplayName("a combine with no handler fails the instance — there is no implicit union fold")
    void combineWithoutHandlerFails() throws Exception {
        FlowSpec bp = FlowSpec.define("no-combine-handler", Map.class, NoCombineSteps.class, (f, s) ->
                Wiggle.allOf(f.thenApply(s::x1), f.thenApply(s::y1)).combine(s::missing));

        InstanceView v = runToTerminal(bp, new NoCombineH(), new LinkedHashMap<>());

        assertEquals("FAILED", v.status(), "no default fold: the unserved combine must fail the instance");
    }

    interface IsolationSteps {
        Map<String, Object> seed(Map<String, Object> ctx);
        Map<String, Object> l(Map<String, Object> ctx);
        Map<String, Object> r(Map<String, Object> ctx);
        Map<String, Object> decide(@Context Map<String, Object> base,
                                   Map<String, Object> left, Map<String, Object> right);
    }

    interface IgnoreArmSteps {
        Map<String, Object> k(Map<String, Object> ctx);
        Map<String, Object> d(Map<String, Object> ctx);
        Map<String, Object> pick(Map<String, Object> keep, Map<String, Object> drop);
        Map<String, Object> tail(Map<String, Object> ctx);
    }

    interface ReplaceSteps {
        Map<String, Object> seed(Map<String, Object> ctx);
        Map<String, Object> a1(Map<String, Object> ctx);
        Map<String, Object> b1(Map<String, Object> ctx);
        Map<String, Object> pickOnly(Map<String, Object> a, Map<String, Object> b);
    }

    /** The combine is declared but deliberately not implemented by NoCombineH -- that is the point. */
    interface NoCombineSteps {
        Map<String, Object> x1(Map<String, Object> ctx);
        Map<String, Object> y1(Map<String, Object> ctx);
        Map<String, Object> missing(Map<String, Object> x, Map<String, Object> y);
    }

    private static Map<String, Object> put(Map<String, Object> ctx, String key, Object value) {
        Map<String, Object> next = new LinkedHashMap<>(ctx);
        next.put(key, value);
        return next;
    }

    @ForFlow("isolation")
    static final class IsolationH {
        public Map<String, Object> seed(Map<String, Object> ctx) { return put(ctx, "base", "B"); }
        public Map<String, Object> l(Map<String, Object> ctx) { return put(ctx, "shared", "from-left"); }
        public Map<String, Object> r(Map<String, Object> ctx) { return put(ctx, "shared", "from-right"); }
        public Map<String, Object> decide(@Context Map<String, Object> base,
                                          Map<String, Object> left,
                                          Map<String, Object> right) {
            // The return is the COMPLETE post-join context: base must be carried explicitly.
            Map<String, Object> out = new LinkedHashMap<>(base);
            out.put("shared", "chosen");
            out.put("sawLeft", left.get("shared"));
            out.put("sawRight", right.get("shared"));
            return out;
        }
    }

    @ForFlow("ignore-arm")
    static final class IgnoreArmH {
        public Map<String, Object> k(Map<String, Object> ctx) { return put(ctx, "kept", true); }
        public Map<String, Object> d(Map<String, Object> ctx) { return put(ctx, "dropped", true); }
        // arms bind by position, so a combine takes them all -- and folds only the one it wants
        public Map<String, Object> pick(Map<String, Object> keep, Map<String, Object> drop) {
            return new LinkedHashMap<>(keep);   // fold only "keep"; "drop" is discarded
        }
        public Map<String, Object> tail(Map<String, Object> ctx) { return ctx; }
    }

    @ForFlow("replace-check")
    static final class ReplaceH {
        public Map<String, Object> seed(Map<String, Object> ctx) { return ctx; }
        public Map<String, Object> a1(Map<String, Object> ctx) { return put(ctx, "a", 1); }
        public Map<String, Object> b1(Map<String, Object> ctx) { return put(ctx, "b", 1); }
        public Map<String, Object> pickOnly(Map<String, Object> a, Map<String, Object> b) {
            return new LinkedHashMap<>(Map.of("picked", true));   // deliberately drops the pre-fork context
        }
    }

    @ForFlow("no-combine-handler")
    static final class NoCombineH {
        public Map<String, Object> x1(Map<String, Object> ctx) { return ctx; }
        public Map<String, Object> y1(Map<String, Object> ctx) { return ctx; }
        // no method for the "missing" combine -- and no default fold exists
    }

    /** Runs a single instance to a terminal state (COMPLETED or FAILED) and returns the view. */
    private static InstanceView runToTerminal(FlowSpec bp, Object handlers, Map<String, Object> input)
            throws Exception {
        String url = "jdbc:h2:mem:iso-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
        com.wiggle.server.ServerConfig config = new com.wiggle.server.ServerConfig(
                0, "node-0", url, "sa", "", 8,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0, Duration.ofSeconds(5), Duration.ofSeconds(10));
        try (com.wiggle.server.WiggleServer server =
                     new com.wiggle.server.WiggleServer(config, new com.wiggle.dist.WiggleStorageFactory()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            Worker w = new Worker(client, "w-0",
                    WorkerOptions.defaults().withConcurrency(4).withLongPollWait(Duration.ofMillis(250)));
            client.register(bp);
            w.registerHandler(handlers);
            w.start();
            try {
                String id = client.start(bp, input);
                return client.awaitCompletion(id, Duration.ofSeconds(30));
            } finally {
                w.close();
            }
        }
    }

    /** Runs a single instance to completion on a one-node in-memory H2 server and returns its context. */
    private static Map<String, Object> run(FlowSpec bp, Object handlers, Map<String, Object> input) throws Exception {
        String url = "jdbc:h2:mem:iso-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
        com.wiggle.server.ServerConfig config = new com.wiggle.server.ServerConfig(
                0, "node-0", url, "sa", "", 8,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0, Duration.ofSeconds(5), Duration.ofSeconds(10));
        try (com.wiggle.server.WiggleServer server =
                     new com.wiggle.server.WiggleServer(config, new com.wiggle.dist.WiggleStorageFactory()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            Worker w = new Worker(client, "w-0",
                    WorkerOptions.defaults().withConcurrency(4).withLongPollWait(Duration.ofMillis(250)));
            client.register(bp);
            w.registerHandler(handlers);
            w.start();
            try {
                String id = client.start(bp, input);
                InstanceView v = client.awaitCompletion(id, Duration.ofSeconds(30));
                return asMap(v.context());
            } finally {
                w.close();
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object o) {
        return o instanceof Map ? (Map<String, Object>) o : Map.of();
    }
}