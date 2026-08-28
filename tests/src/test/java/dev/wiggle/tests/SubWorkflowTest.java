package dev.wiggle.tests;

import dev.wiggle.client.dsl.Blueprint;
import dev.wiggle.client.dsl.Workflow;
import dev.wiggle.client.WiggleClient;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Sub-workflows: a node starts a child instance with the parent's context, the parent waits,
 * and the child's outcome (final context, failure, cancellation) propagates back.
 */
class SubWorkflowTest {

    private static Map<String, Object> put(Map<String, Object> ctx, String k, Object v) {
        Map<String, Object> n = new LinkedHashMap<>(ctx);
        n.put(k, v);
        return n;
    }

    private static ServerConfig config() {
        return new ServerConfig(0, "sub-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    private static Blueprint<Map<String, Object>> parent() {
        return Workflow.define("sub-parent")
                .step("prepare", ctx -> put(ctx, "prepared", true))
                .subWorkflow("delegate", "sub-child")
                .step("wrap-up", ctx -> put(ctx, "wrapped", true))
                .build();
    }

    @Test @DisplayName("the child runs with the parent's context and its result merges back")
    void childCompletes() throws Exception {
        Blueprint<Map<String, Object>> child = Workflow.define("sub-child")
                .step("child-work", ctx -> put(ctx, "childSaw", ctx.get("prepared")))
                .step("child-done", ctx -> put(ctx, "childResult", 42L))
                .build();

        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker w = new Worker(client, "sub-w").register(parent()).register(child)) {
            w.start();
            InstanceView v = client.awaitCompletion(client.start(parent(), Map.of("input", 1L)),
                    Duration.ofSeconds(20));
            assertEquals("COMPLETED", v.status());
            Map<String, Object> ctx = Json.asObject(v.context());
            assertEquals(true, ctx.get("prepared"));
            assertEquals(true, ctx.get("childSaw"), "the child started from the parent's context");
            assertEquals(42L, ctx.get("childResult"), "the child's writes merged back");
            assertEquals(true, ctx.get("wrapped"), "the parent resumed after the child");
        }
    }

    @Test @DisplayName("a failing child fails the parent with the child's error")
    void childFailureFailsParent() throws Exception {
        Blueprint<Map<String, Object>> child = Workflow.define("sub-child")
                .step("child-work", ctx -> { throw new IllegalStateException("child broke"); },
                        dev.wiggle.core.RetryPolicy.fixed(1, Duration.ofMillis(1)))
                .build();

        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker w = new Worker(client, "sub-w2").register(parent()).register(child)) {
            w.start();
            InstanceView v = client.awaitCompletion(client.start(parent(), Map.of()), Duration.ofSeconds(20));
            assertEquals("FAILED", v.status());
            assertTrue(v.error().contains("sub-workflow 'sub-child' FAILED"), v.error());
            assertTrue(v.error().contains("child broke"), "carries the child's error");
        }
    }

    @Test @DisplayName("an unregistered child workflow fails the parent immediately")
    void unregisteredChildFailsParent() throws Exception {
        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker w = new Worker(client, "sub-w3").register(parent())) {
            w.start();
            InstanceView v = client.awaitCompletion(client.start(parent(), Map.of()), Duration.ofSeconds(20));
            assertEquals("FAILED", v.status());
            assertTrue(v.error().contains("sub-workflow 'sub-child'"), v.error());
        }
    }

    @Test @DisplayName("cancelling the parent cascades to the running child")
    void cancelCascades() throws Exception {
        Blueprint<Map<String, Object>> child = Workflow.define("sub-child")
                .awaitSignal("never-arrives")   // the child parks so it is definitely still running
                .step("child-done", ctx -> ctx)
                .build();

        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker w = new Worker(client, "sub-w4").register(parent()).register(child)) {
            w.start();
            String parentId = client.start(parent(), Map.of());

            // Wait until the child is parked on its signal (proves it is running).
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (server.engine().pendingSignals(10).isEmpty() && System.nanoTime() < deadline) {
                Thread.sleep(20);
            }
            String childId = server.engine().pendingSignals(10).get(0).instanceId;

            client.cancel(parentId, "changed my mind");
            assertEquals("CANCELLED", client.instance(parentId).status());
            InstanceView childView = client.awaitCompletion(childId, Duration.ofSeconds(10));
            assertEquals("CANCELLED", childView.status(), "the child was cancelled with its parent");
        }
    }
}
