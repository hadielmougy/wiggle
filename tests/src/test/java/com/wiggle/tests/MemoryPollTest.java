package com.wiggle.tests;

import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.client.worker.PollResult;
import com.wiggle.client.WiggleClient;
import com.wiggle.core.Ids;
import com.wiggle.core.Tls;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end memory-pressure admission control. Over the threshold the server rejects a configured
 * fraction of polls (empty + a jittered hold-off) before doing any work; under the threshold every
 * poll is served normally. Uses a reject ratio of 1.0 (reject all) with the threshold set below any
 * live JVM's heap usage, so the reject/serve decision is deterministic.
 */
class MemoryPollTest {

    private static ServerConfig config(double threshold, double rejectRatio) {
        ServerConfig.Memory memory = new ServerConfig.Memory(
                true, threshold, rejectRatio, Duration.ofMillis(200), Duration.ofMillis(100));
        return new ServerConfig(0, "mem-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                /*maxLongPoll*/ Duration.ofMillis(2_000), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10), "admin", null, Tls.Options.DISABLED, memory);
    }

    @Test @DisplayName("over the memory threshold, a rejected poll returns empty + hold-off even when work exists")
    void rejectsUnderPressure() throws Exception {
        Blueprint bp = Workflow.define("mem-" + Ids.next("wf"))
                .step("work", ctx -> ctx)
                .build();
        // 0.0001 is below any running JVM's live-set/max, so the guard is always under pressure;
        // reject ratio 1.0 => every poll is rejected -- deterministic.
        try (WiggleServer server = new WiggleServer(config(0.0001, 1.0)).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            client.register(bp);
            client.start(bp, Map.of());   // there IS claimable work, but the poll is still rejected

            // Collection usage is only populated after a GC; force one so the live-set reading is set.
            System.gc();
            Thread.sleep(100);

            long t0 = System.currentTimeMillis();
            PollResult r = client.poll("w", bp.definition().workerQueues(), 1, 30_000, 2_000);
            long elapsed = System.currentTimeMillis() - t0;
            assertTrue(r.tasks().isEmpty(), "the poll was rejected, so no task is handed out");
            assertTrue(r.retryAfterMillis() >= 200 && r.retryAfterMillis() <= 300,
                    "jittered hold-off, got " + r.retryAfterMillis());
            assertTrue(elapsed < 1_500, "rejected promptly, not long-polled, took " + elapsed + "ms");
        }
    }

    @Test @DisplayName("end to end, under pressure the observed reject rate matches the configured ratio")
    void observedRejectRateMatchesRatio() throws Exception {
        double ratio = 0.30;
        // Under pressure (threshold below live-set/max) with a 30% reject ratio; wait=0 so served
        // polls return an immediate empty (retry 0), and rejected polls an immediate empty (retry>0).
        try (WiggleServer server = new WiggleServer(config(0.0001, ratio)).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            System.gc();
            Thread.sleep(100);

            int n = 2_000, rejected = 0;
            for (int i = 0; i < n; i++) {
                PollResult r = client.poll("w" + i, java.util.Set.of("q-idle"), 1, 30_000, 0);
                assertTrue(r.tasks().isEmpty(), "no work, so every poll is empty");
                if (r.retryAfterMillis() > 0) {
                    assertTrue(r.retryAfterMillis() >= 200 && r.retryAfterMillis() <= 300,
                            "rejected polls carry the jittered hold-off, got " + r.retryAfterMillis());
                    rejected++;
                }
            }
            double observed = (double) rejected / n;
            System.out.println("MemoryPollTest: configured reject ratio " + ratio
                    + ", observed " + observed + " (" + rejected + "/" + n + ")");
            assertTrue(observed > 0.25 && observed < 0.35,
                    "observed reject rate " + observed + " should be near the configured " + ratio);
        }
    }

    @Test @DisplayName("under a normal threshold no poll is rejected and work flows")
    void noRejectUnderThreshold() throws Exception {
        Blueprint bp = Workflow.define("mem-ok-" + Ids.next("wf"))
                .step("work", ctx -> ctx)
                .build();
        // Threshold 0.999 is effectively never crossed, so even reject-ratio 1.0 never triggers.
        try (WiggleServer server = new WiggleServer(config(0.999, 1.0)).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            client.register(bp);
            client.start(bp, Map.of());
            PollResult r = client.poll("w", bp.definition().workerQueues(), 1, 30_000, 2_000);
            assertEquals(1, r.tasks().size(), "work delivered normally");
            assertEquals(0, r.retryAfterMillis(), "no hold-off when not under pressure");
        }
    }
}
