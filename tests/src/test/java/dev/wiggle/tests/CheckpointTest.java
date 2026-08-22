package dev.wiggle.tests;

import dev.wiggle.client.dsl.Blueprint;
import dev.wiggle.client.dsl.Workflow;
import dev.wiggle.client.worker.WiggleClient;
import dev.wiggle.client.worker.Worker;
import dev.wiggle.core.ExecutionMode;
import dev.wiggle.core.InstanceView;
import dev.wiggle.core.Json;
import dev.wiggle.server.ServerConfig;
import dev.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** {@code .checkpoint()} plumbing (version hash) and its LOCAL_ASYNC behaviour (forces an early flush). */
class CheckpointTest {

    private static Map<String, Object> put(Map<String, Object> ctx, String k, Object v) {
        Map<String, Object> n = new LinkedHashMap<>(ctx);
        n.put(k, v);
        return n;
    }

    private static ServerConfig config() {
        return new ServerConfig(0, "cp-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    @Test @DisplayName("checkpoint is recorded, changes the content hash, and must follow a step")
    void plumbing() {
        Blueprint<Map<String, Object>> plain = Workflow.defineJson("cp")
                .step("a", ctx -> ctx).step("b", ctx -> ctx).build();
        Blueprint<Map<String, Object>> checked = Workflow.defineJson("cp")
                .step("a", ctx -> ctx).checkpoint().step("b", ctx -> ctx).build();

        assertTrue(plain.definition().checkpoints().isEmpty(), "no checkpoints by default");
        assertEquals(1, checked.definition().checkpoints().size(), "one checkpoint recorded");
        assertNotEquals(plain.version(), checked.version(), "checkpoint is part of the content hash");

        assertThrows(IllegalStateException.class, () -> Workflow.defineJson("bad").checkpoint(),
                "checkpoint() must follow a step");
    }

    @Test @DisplayName("LOCAL_ASYNC: a checkpoint commits its step before the next one runs")
    void checkpointForcesFlush() throws Exception {
        CountDownLatch bRunning = new CountDownLatch(1);
        CountDownLatch releaseB = new CountDownLatch(1);

        Blueprint<Map<String, Object>> bp = Workflow.defineJson("cp-flush")
                .execution(ExecutionMode.LOCAL_ASYNC)
                .step("a", ctx -> put(ctx, "a", 1L)).checkpoint()
                .step("b", ctx -> { bRunning.countDown(); await(releaseB); return put(ctx, "b", 2L); })
                .step("c", ctx -> put(ctx, "c", 3L))
                .build();

        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker w = new Worker(client, "cp-w").register(bp)) {
            w.start();
            String id = client.start(bp, Map.of());

            assertTrue(bRunning.await(10, TimeUnit.SECONDS), "worker reached step b");
            // The checkpoint after 'a' flushed it before 'b' started, so 'a' is already durable.
            Map<String, Object> mid = Json.asObject(server.engine().instance(id).orElseThrow().context());
            assertEquals(1L, mid.get("a"), "checkpointed step committed before the next ran");
            assertNull(mid.get("b"), "the still-running step is not committed yet");

            releaseB.countDown();
            InstanceView v = client.awaitCompletion(id, Duration.ofSeconds(20));
            assertEquals("COMPLETED", v.status());
            Map<String, Object> ctx = Json.asObject(v.context());
            assertEquals(1L, ctx.get("a"));
            assertEquals(2L, ctx.get("b"));
            assertEquals(3L, ctx.get("c"));
        }
    }

    @Test @DisplayName("LOCAL_ASYNC without a checkpoint buffers the step (not yet committed mid-run)")
    void withoutCheckpointBuffers() throws Exception {
        CountDownLatch bRunning = new CountDownLatch(1);
        CountDownLatch releaseB = new CountDownLatch(1);

        Blueprint<Map<String, Object>> bp = Workflow.defineJson("cp-nobuf")
                .execution(ExecutionMode.LOCAL_ASYNC)
                .step("a", ctx -> put(ctx, "a", 1L))   // no checkpoint
                .step("b", ctx -> { bRunning.countDown(); await(releaseB); return put(ctx, "b", 2L); })
                .step("c", ctx -> put(ctx, "c", 3L))
                .build();

        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker w = new Worker(client, "cp-w2").register(bp)) {
            w.start();
            String id = client.start(bp, Map.of());

            assertTrue(bRunning.await(10, TimeUnit.SECONDS), "worker reached step b");
            // 'a' is still buffered (async flushes at the boundary), so nothing is committed yet.
            Map<String, Object> mid = Json.asObject(server.engine().instance(id).orElseThrow().context());
            assertNull(mid.get("a"), "without a checkpoint, 'a' stays buffered until the run flushes");

            releaseB.countDown();
            InstanceView v = client.awaitCompletion(id, Duration.ofSeconds(20));
            assertEquals("COMPLETED", v.status());
            Map<String, Object> ctx = Json.asObject(v.context());
            assertEquals(1L, ctx.get("a"));
            assertEquals(3L, ctx.get("c"));
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(20, TimeUnit.SECONDS)) throw new IllegalStateException("latch not released");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
