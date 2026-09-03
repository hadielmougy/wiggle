package com.wiggle.tests;

import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.client.WiggleClient;
import com.wiggle.client.worker.Worker;
import com.wiggle.core.ExecutionMode;
import com.wiggle.core.Ids;
import com.wiggle.core.InstanceView;
import com.wiggle.core.Json;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Closing a worker mid-{@code LOCAL_ASYNC}-run must not lose already-computed steps: the buffer
 * is drained to the server instead of discarded, and a fresh worker finishes the rest -- with
 * every step running exactly once.
 */
class GracefulShutdownTest {

    private static Map<String, Object> put(Map<String, Object> ctx, String k, Object v) {
        Map<String, Object> n = new LinkedHashMap<>(ctx);
        n.put(k, v);
        return n;
    }

    private static ServerConfig config() {
        return new ServerConfig(0, "shutdown-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    @Test @DisplayName("close() drains buffered LOCAL_ASYNC steps instead of discarding them")
    void closeDrainsBufferedSteps() throws Exception {
        AtomicInteger runsOfA = new AtomicInteger();
        AtomicInteger runsOfB = new AtomicInteger();
        AtomicInteger runsOfC = new AtomicInteger();
        CountDownLatch aStarted = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);

        // Default batch size (64) means step "a" alone never triggers a flush -- its result sits
        // only in the worker's in-memory buffer until a boundary, a full batch, or a drain.
        Blueprint bp = Workflow.define("shutdown-drain")
                .execution(ExecutionMode.LOCAL_ASYNC)
                .step("a", ctx -> {
                    runsOfA.incrementAndGet();
                    aStarted.countDown();
                    await(proceed);
                    return put(ctx, "a", 1L);
                })
                .step("b", ctx -> { runsOfB.incrementAndGet(); return put(ctx, "b", 2L); })
                .step("c", ctx -> { runsOfC.incrementAndGet(); return put(ctx, "c", 3L); })
                .build();

        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            Worker worker = new Worker(client, "w-" + Ids.next("x")).register(bp);
            worker.start();
            String id = client.start(bp, Map.of());

            assertTrue(aStarted.await(10, TimeUnit.SECONDS), "step 'a' started");

            // Close on a separate thread: close() blocks until the in-flight step finishes, and
            // that step is deliberately parked on `proceed` right now.
            Thread closer = new Thread(worker::close);
            closer.start();
            // Deterministically wait until close() has flipped the running flag (rather than guessing
            // with a sleep), so step "a" only returns once the drain precondition holds -- robust under
            // heavy parallel test load.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (worker.isRunning() && System.nanoTime() < deadline) Thread.sleep(2);
            assertTrue(!worker.isRunning(), "close() flipped the running flag");
            proceed.countDown();
            closer.join(TimeUnit.SECONDS.toMillis(20));
            assertTrue(!closer.isAlive(), "close() returned once the drain completed");

            // The instance is not done -- "b" and "c" never ran -- but "a" already landed.
            InstanceView mid = server.engine().instance(id).orElseThrow();
            assertEquals("RUNNING", mid.status());
            assertEquals(1L, Json.asObject(mid.context()).get("a"), "the drained step is committed, not lost");

            // A fresh worker picks up right where the drain left off.
            try (Worker second = new Worker(client, "w-" + Ids.next("x")).register(bp)) {
                second.start();
                InstanceView v = client.awaitCompletion(id, Duration.ofSeconds(20));
                assertEquals("COMPLETED", v.status());
                Map<String, Object> ctx = Json.asObject(v.context());
                assertEquals(1L, ctx.get("a"));
                assertEquals(2L, ctx.get("b"));
                assertEquals(3L, ctx.get("c"));
            }

            assertEquals(1, runsOfA.get(), "'a' ran exactly once despite the shutdown");
            assertEquals(1, runsOfB.get(), "'b' ran exactly once");
            assertEquals(1, runsOfC.get(), "'c' ran exactly once");
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
