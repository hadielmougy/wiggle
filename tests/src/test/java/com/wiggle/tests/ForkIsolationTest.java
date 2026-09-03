package com.wiggle.tests;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Branch;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.client.worker.Arm;
import com.wiggle.client.worker.Handlers;
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
 * The design-B guarantee of {@link com.wiggle.client.dsl.WorkflowStream#fork}: each branch runs on
 * its own isolated context copy, so a branch's writes are invisible to its siblings and never reach
 * the shared context implicitly -- the only thing that lands is what the mandatory {@code combine}
 * returns.
 */
class ForkIsolationTest {

    @Test @DisplayName("branch writes are isolated: no implicit merge, combine owns what lands")
    void branchesAreIsolatedAndCombineDecides() throws Exception {
        Blueprint bp = Workflow.define("isolation")
                .step("seed")
                .fork(
                        // Both arms write the SAME key to different values, and each also asserts it
                        // cannot see the base being overwritten by its sibling (isolation).
                        Branch.of("left", s -> s.step("l")),
                        Branch.of("right", s -> s.step("r")))
                // The combine ignores both arms' "shared" writes entirely and sets its own value,
                // proving nothing merges unless combine returns it.
                .combine("decide")
                .build();

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
        Blueprint bp = Workflow.define("ignore-arm")
                .fork(
                        Branch.of("keep", s -> s.step("k")),
                        Branch.of("drop", s -> s.step("d")))
                // Only "keep" is folded back; "drop"'s writes are discarded with its isolated context.
                .combine("pick")
                .step("tail")
                .build();

        Map<String, Object> out = run(bp, new IgnoreArmH(), new LinkedHashMap<>());

        assertEquals(true, out.get("kept"));
        assertFalse(out.containsKey("dropped"), "the ignored branch left no trace: " + out);
    }

    private static Map<String, Object> put(Map<String, Object> ctx, String key, Object value) {
        Map<String, Object> next = new LinkedHashMap<>(ctx);
        next.put(key, value);
        return next;
    }

    @Handlers("isolation")
    static final class IsolationH {
        public Map<String, Object> seed(Map<String, Object> ctx) { return put(ctx, "base", "B"); }
        public Map<String, Object> l(Map<String, Object> ctx) { return put(ctx, "shared", "from-left"); }
        public Map<String, Object> r(Map<String, Object> ctx) { return put(ctx, "shared", "from-right"); }
        public Map<String, Object> decide(@Arm("left") Map<String, Object> left,
                                          @Arm("right") Map<String, Object> right) {
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("shared", "chosen");
            out.put("sawLeft", left.get("shared"));
            out.put("sawRight", right.get("shared"));
            return out;
        }
    }

    @Handlers("ignore-arm")
    static final class IgnoreArmH {
        public Map<String, Object> k(Map<String, Object> ctx) { return put(ctx, "kept", true); }
        public Map<String, Object> d(Map<String, Object> ctx) { return put(ctx, "dropped", true); }
        public Map<String, Object> pick(@Arm("keep") Map<String, Object> keep) {
            return new LinkedHashMap<>(keep);   // fold only "keep"; "drop" is discarded
        }
        public Map<String, Object> tail(Map<String, Object> ctx) { return ctx; }
    }

    /** Runs a single instance to completion on a one-node in-memory H2 server and returns its context. */
    private static Map<String, Object> run(Blueprint bp, Object handlers, Map<String, Object> input) throws Exception {
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
            w.register(bp).handlers(handlers);
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