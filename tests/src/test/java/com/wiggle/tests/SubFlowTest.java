package com.wiggle.tests;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.WiggleClient;
import com.wiggle.client.worker.Handlers;
import com.wiggle.client.worker.Worker;
import com.wiggle.core.InstanceView;
import com.wiggle.core.Json;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
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
class SubFlowTest {

    /** The steps this spec names; a worker binds them by name. */
    interface ParentSteps {
        Map<String, Object> prepare(Map<String, Object> ctx);
        Map<String, Object> wrapUp(Map<String, Object> ctx);
    }

    interface OneStep {
        Map<String, Object> childDone(Map<String, Object> ctx);
        Map<String, Object> childWork(Map<String, Object> ctx);
    }

    private static Map<String, Object> put(Map<String, Object> ctx, String k, Object v) {
        Map<String, Object> n = new LinkedHashMap<>(ctx);
        n.put(k, v);
        return n;
    }

    private static ServerConfig config() {
        return new ServerConfig(TestPorts.free(), "sub-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    private static FlowSpec parent() {
        return FlowSpec.define("sub-parent", Map.class, ParentSteps.class, (f, s) -> f
                .thenApply(s::prepare)
                .thenSubFlow("delegate", "sub-child", Map.class)
                .thenApply(s::wrapUp));
    }

    @Handlers("sub-parent")
    static final class ParentH {
        public Map<String, Object> prepare(Map<String, Object> c) { return put(c, "prepared", true); }
        public Map<String, Object> wrapUp(Map<String, Object> c) { return put(c, "wrapped", true); }
    }

    @Handlers("sub-child")
    static final class ChildOkH {
        public Map<String, Object> childWork(Map<String, Object> c) { return put(c, "childSaw", c.get("prepared")); }
        public Map<String, Object> childDone(Map<String, Object> c) { return put(c, "childResult", 42L); }
    }

    @Handlers("sub-child")
    static final class ChildFailH {
        public Map<String, Object> childWork(Map<String, Object> c) { throw new IllegalStateException("child broke"); }
    }

    @Handlers("sub-child")
    static final class ChildParkH {
        public Map<String, Object> childDone(Map<String, Object> c) { return c; }
    }

    @Test @DisplayName("the child runs with the parent's context and its result merges back")
    void childCompletes() throws Exception {
        FlowSpec child = FlowSpec.define("sub-child", Map.class, OneStep.class, (f, s) -> f
                .thenApply(s::childWork)
                .thenApply(s::childDone));

        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker w = new Worker(client, "sub-w")
                     .handlers(new ParentH()).handlers(new ChildOkH())) {
            client.register(child);
            client.register(parent());
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
        FlowSpec child = FlowSpec.define("sub-child", Map.class, OneStep.class, (f, s) -> f
                .thenApply(s::childWork, com.wiggle.core.RetryPolicy.fixed(1, Duration.ofMillis(1))));

        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker w = new Worker(client, "sub-w2")
                     .handlers(new ParentH()).handlers(new ChildFailH())) {
            client.register(child);
            client.register(parent());
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
             Worker w = new Worker(client, "sub-w3").handlers(new ParentH())) {
            client.register(parent());   // the child is deliberately NOT registered
            w.start();
            InstanceView v = client.awaitCompletion(client.start(parent(), Map.of()), Duration.ofSeconds(20));
            assertEquals("FAILED", v.status());
            assertTrue(v.error().contains("sub-workflow 'sub-child'"), v.error());
        }
    }

    @Test @DisplayName("cancelling the parent cascades to the running child")
    void cancelCascades() throws Exception {
        FlowSpec child = FlowSpec.define("sub-child", Map.class, OneStep.class, (f, s) -> f
                .thenAwait("never-arrives")   // the child parks so it is definitely still running
                .thenApply(s::childDone));

        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker w = new Worker(client, "sub-w4")
                     .handlers(new ParentH()).handlers(new ChildParkH())) {
            client.register(child);
            client.register(parent());
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
