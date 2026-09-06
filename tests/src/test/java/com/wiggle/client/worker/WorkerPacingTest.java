package com.wiggle.client.worker;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.client.dsl.WorkflowBuilder;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Poll-loop pacing: a saturated worker must resume polling the moment a task slot frees, not after a
 * fixed idle-backoff nap. Before the capacity-wake, a worker at full concurrency slept
 * {@code idleBackoff} (default 200ms) per wave even though its (trivial) steps finished in
 * milliseconds -- gating SERVER-mode throughput to one concurrency-sized wave per nap.
 */
class WorkerPacingTest {

    private static final int STEPS = 10;
    private static final int INSTANCES = 6;
    private static final int CONCURRENCY = 2;

    /** All hop steps canonicalise to the one {@code hop} handler (punctuation is ignored on match). */
    @Handlers("pacing")
    static final class PacingH {
        public Map<String, Object> hop(Map<String, Object> ctx) { return ctx; }
    }

    private static Blueprint chain() {
        WorkflowBuilder b = Workflow.define("pacing");
        for (int i = 0; i < STEPS; i++) b = b.step("hop" + "-".repeat(i));
        return b.build();
    }

    @Test @DisplayName("a saturated worker drains promptly (no idle-backoff wave gating)")
    void saturatedWorkerDrainsPromptly() throws Exception {
        ServerConfig config = new ServerConfig(0, "pacing-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
        try (WiggleServer server = new WiggleServer(config).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            client.register(chain());
            // Pre-submit so the worker is saturated from its first poll.
            String[] ids = new String[INSTANCES];
            for (int i = 0; i < INSTANCES; i++) ids[i] = client.start("pacing", Map.of());

            try (Worker w = new Worker(client, "pacing-w",
                    WorkerOptions.defaults().withConcurrency(CONCURRENCY))) {
                w.handlers(new PacingH()).register(chain());
                long t0 = System.nanoTime();
                w.start();
                for (String id : ids) {
                    assertEquals("COMPLETED", client.awaitCompletion(id, Duration.ofSeconds(15)).status());
                }
                double seconds = (System.nanoTime() - t0) / 1e9;
                // 60 trivial completions at concurrency 2. The old fixed-nap pacing needed >= one
                // idle-backoff (200ms) per 2-task wave => >= ~6s; capacity-wake should be far under it.
                assertTrue(seconds < 3.5, "drained in " + seconds + "s -- poll loop still wave-gated?");
            }
        }
    }
}
