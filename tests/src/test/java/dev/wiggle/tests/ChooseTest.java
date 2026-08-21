package dev.wiggle.tests;

import dev.wiggle.client.dsl.Blueprint;
import dev.wiggle.client.dsl.Case;
import dev.wiggle.client.dsl.Workflow;
import dev.wiggle.client.worker.WiggleClient;
import dev.wiggle.client.worker.Worker;
import dev.wiggle.core.InstanceView;
import dev.wiggle.core.Json;
import dev.wiggle.server.ServerConfig;
import dev.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Exclusive-choice ({@code choose}) behaviour: first match wins, exactly one branch runs. */
class ChooseTest {

    private static Map<String, Object> put(Map<String, Object> ctx, String k, Object v) {
        Map<String, Object> n = new LinkedHashMap<>(ctx);
        n.put(k, v);
        return n;
    }

    private final Map<String, AtomicInteger> ran = new ConcurrentHashMap<>();

    private AtomicInteger counter(String name) {
        return ran.computeIfAbsent(name, k -> new AtomicInteger());
    }

    /** choose with a default; the "gold" and "premium" guards deliberately overlap to prove first-match. */
    private Blueprint<Map<String, Object>> withDefault() {
        return Workflow.defineJson("choose-default")
                .choose(
                        Case.when("is-gold", c -> "gold".equals(c.get("tier")),
                                b -> b.map("gold", c -> { counter("gold").incrementAndGet(); return put(c, "path", "gold"); })),
                        Case.when("is-premium", c -> c.get("tier") != null,   // also true for "gold": must not win
                                b -> b.map("premium", c -> { counter("premium").incrementAndGet(); return put(c, "path", "premium"); })),
                        Case.otherwise("plain",
                                b -> b.map("plain", c -> { counter("plain").incrementAndGet(); return put(c, "path", "plain"); })))
                .map("finalize", c -> { counter("finalize").incrementAndGet(); return put(c, "done", true); })
                .build();
    }

    /** choose without a default: an unmatched context skips straight to the continuation. */
    private Blueprint<Map<String, Object>> withoutDefault() {
        return Workflow.defineJson("choose-skip")
                .choose(
                        Case.when("is-a", c -> "a".equals(c.get("k")),
                                b -> b.map("a", c -> put(c, "path", "a"))))
                .map("finalize", c -> put(c, "done", true))
                .build();
    }

    private void withServer(Blueprint<Map<String, Object>> bp, java.util.function.BiConsumer<WiggleClient, Blueprint<Map<String, Object>>> body) throws Exception {
        ServerConfig config = new ServerConfig(0, "test-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100);
        try (WiggleServer server = new WiggleServer(config).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker w = new Worker(client, "w-choose").register(bp)) {
            w.start();
            body.accept(client, bp);
        }
    }

    private Map<String, Object> run(WiggleClient client, Blueprint<Map<String, Object>> bp, Map<String, Object> input) {
        InstanceView v = client.awaitCompletion(client.start(bp, input), Duration.ofSeconds(20));
        assertEquals("COMPLETED", v.status(), "status");
        return Json.asObject(v.context());
    }

    @Test @DisplayName("the first matching guard wins and only its branch runs")
    void firstMatchWins() throws Exception {
        ran.clear();
        withServer(withDefault(), (client, bp) -> {
            Map<String, Object> out = run(client, bp, Map.of("tier", "gold"));
            assertEquals("gold", out.get("path"), "gold branch chosen");
            assertEquals(true, out.get("done"), "continuation ran");
            assertEquals(1, counter("gold").get(), "gold ran once");
            assertEquals(0, counter("premium").get(), "overlapping later guard did not run");
            assertEquals(0, counter("plain").get(), "default did not run");
            assertEquals(1, counter("finalize").get(), "continuation ran exactly once");
        });
    }

    @Test @DisplayName("a later guard runs when earlier ones miss")
    void laterGuardMatches() throws Exception {
        ran.clear();
        withServer(withDefault(), (client, bp) -> {
            Map<String, Object> out = run(client, bp, Map.of("tier", "silver"));
            assertEquals("premium", out.get("path"), "second guard chosen");
            assertEquals(0, counter("gold").get());
            assertEquals(1, counter("premium").get());
            assertEquals(0, counter("plain").get());
        });
    }

    @Test @DisplayName("the default branch runs when no guard matches")
    void defaultRuns() throws Exception {
        ran.clear();
        withServer(withDefault(), (client, bp) -> {
            Map<String, Object> out = run(client, bp, Map.of());   // no tier
            assertEquals("plain", out.get("path"), "default branch chosen");
            assertEquals(1, counter("plain").get());
            assertEquals(0, counter("gold").get());
            assertEquals(0, counter("premium").get());
        });
    }

    @Test @DisplayName("with no default, an unmatched context skips straight to the continuation")
    void noMatchSkips() throws Exception {
        withServer(withoutDefault(), (client, bp) -> {
            Map<String, Object> out = run(client, bp, Map.of("k", "other"));
            assertNull(out.get("path"), "no branch ran");
            assertEquals(true, out.get("done"), "continuation still ran");
        });
    }
}
