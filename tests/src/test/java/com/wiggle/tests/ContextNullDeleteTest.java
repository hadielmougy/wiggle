package com.wiggle.tests;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Branch;
import com.wiggle.client.dsl.Workflow;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The engine merges a task result into the context with delete-on-null semantics (the inverse of
 * {@link com.wiggle.core.Json#shallowDiff}): a null value removes its key rather than persisting a
 * JSON null. Exercised end-to-end for a step that drops a field, and for a {@code fork}'s combine
 * clearing its per-branch scratch keys.
 */
class ContextNullDeleteTest {

    @Test @DisplayName("a step that drops a field removes it from the context (not left as null)")
    void droppedFieldIsRemoved() throws Exception {
        Blueprint bp = Workflow.define("trim")
                .step("trim", ctx -> {
                    Map<String, Object> next = new LinkedHashMap<>(ctx);
                    next.remove("drop");            // shallowDiff emits drop -> null
                    return next;
                })
                .build();

        Map<String, Object> in = new LinkedHashMap<>();
        in.put("keep", 1);
        in.put("drop", 2);
        Map<String, Object> out = run(bp, in);

        assertEquals(1, ((Number) out.get("keep")).intValue());
        assertFalse(out.containsKey("drop"), "dropped key must be gone, not a lingering null: " + out);
    }

    @Test @DisplayName("branch combine clears its per-branch scratch keys from the final context")
    void combineScratchKeysAreRemoved() throws Exception {
        Blueprint bp = Workflow.define("trip")
                .fork(
                        Branch.of("air", s -> s.step("air", ctx -> Map.of("price", 100))),
                        Branch.of("hotel", s -> s.step("hotel", ctx -> Map.of("price", 75))))
                .combine("merge", (ctx, parts) -> Map.of("total",
                        price(parts.get("air")) + price(parts.get("hotel"))))
                .build();

        Map<String, Object> out = run(bp, new LinkedHashMap<>(Map.of("id", "t1")));

        assertEquals(175, ((Number) out.get("total")).intValue());
        assertTrue(out.containsKey("id"));
        assertFalse(out.containsKey("air"), "air scratch key must be removed: " + out);
        assertFalse(out.containsKey("hotel"), "hotel scratch key must be removed: " + out);
    }

    @SuppressWarnings("unchecked")
    private static int price(Object branchOutput) {
        return ((Number) ((Map<String, Object>) branchOutput).get("price")).intValue();
    }

    /** Runs a single instance to completion on a one-node, in-memory H2 server and returns its context. */
    private static Map<String, Object> run(Blueprint bp, Map<String, Object> input) throws Exception {
        String url = "jdbc:h2:mem:nulldel-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
        com.wiggle.server.ServerConfig config = new com.wiggle.server.ServerConfig(
                0, "node-0", url, "sa", "", 8,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0, Duration.ofSeconds(5), Duration.ofSeconds(10));
        try (com.wiggle.server.WiggleServer server =
                     new com.wiggle.server.WiggleServer(config, new com.wiggle.dist.WiggleStorageFactory()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            Worker w = new Worker(client, "w-0",
                    WorkerOptions.defaults().withConcurrency(4).withLongPollWait(Duration.ofMillis(250)));
            w.register(bp);
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
