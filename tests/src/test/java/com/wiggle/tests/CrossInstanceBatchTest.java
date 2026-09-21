package com.wiggle.tests;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.flow.Wiggle;
import com.wiggle.client.worker.ForFlow;
import com.wiggle.client.worker.Worker;
import com.wiggle.client.worker.WorkerOptions;
import com.wiggle.core.ExecutionMode;
import com.wiggle.core.InstanceView;
import com.wiggle.core.Json;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The worker's handback batcher, end to end: a LOCAL_ASYNC fork gives one worker two runs of the
 * SAME instance, and under concurrency their final handbacks land in the same AdvanceMany
 * window -- the batch keeps the first and refuses the second, whose submitting thread falls back
 * to the single-run path. Every instance must still complete with the combine's exact context;
 * a lost fallback shows up here as a stuck join.
 */
class CrossInstanceBatchTest {

    /** The steps this spec names; a worker binds them by name. */
    interface ForkSteps {
        Map<String, Object> a(Map<String, Object> ctx);
        Map<String, Object> b(Map<String, Object> ctx);
        Map<String, Object> pick(Map<String, Object> left, Map<String, Object> right);
    }

    @ForFlow("xib-fork")
    public static final class H {
        public Map<String, Object> a(Map<String, Object> ctx) {
            Map<String, Object> n = new LinkedHashMap<>(ctx); n.put("a", 1L); return n;
        }
        public Map<String, Object> b(Map<String, Object> ctx) {
            Map<String, Object> n = new LinkedHashMap<>(ctx); n.put("b", 2L); return n;
        }
        public Map<String, Object> pick(Map<String, Object> left, Map<String, Object> right) {
            Map<String, Object> n = new LinkedHashMap<>();
            n.put("a", left.get("a")); n.put("b", right.get("b")); n.put("picked", true);
            return n;
        }
    }

    private static ServerConfig config() {
        return new ServerConfig(TestPorts.free(), "xib-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    @Test @Timeout(60)
    @DisplayName("forked LOCAL_ASYNC handbacks batch across instances; same-instance arms fall back and the joins fire")
    void forkedHandbacksBatchAndFallBack() throws Exception {
        FlowSpec bp = FlowSpec.define("xib-fork", 1, Map.class, ForkSteps.class, (f, s) ->
                Wiggle.allOf(f.thenApply(s::a), f.thenApply(s::b))
                        .combine(s::pick)
                        .execution(ExecutionMode.LOCAL_ASYNC));
        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker w = new Worker(client, "xib-w",
                     WorkerOptions.defaults().withConcurrency(8)).registerHandler(new H())) {
            client.register(bp);
            w.start();
            List<String> ids = new ArrayList<>();
            for (int i = 0; i < 20; i++) ids.add(client.start(bp, Map.of()));
            for (String id : ids) {
                InstanceView v = client.awaitCompletion(id, Duration.ofSeconds(30));
                assertEquals("COMPLETED", v.status(), id);
                Map<String, Object> ctx = Json.asObject(v.context());
                assertEquals(1L, ctx.get("a"), id);
                assertEquals(2L, ctx.get("b"), id);
                assertEquals(true, ctx.get("picked"), id);
            }
        }
    }

    /**
     * The wire itself, no batcher in between: two claimed tasks reported through one
     * client.advanceMany call. The batcher falls back to singles when this RPC breaks, so the
     * end-to-end test above stays green through a dead wire -- this one does not.
     */
    @Test @Timeout(30)
    @DisplayName("client.advanceMany carries two runs over the wire and both commit")
    void advanceManyOverTheWire() throws Exception {
        interface TwoSteps {
            Map<String, Object> x(Map<String, Object> ctx);
            Map<String, Object> y(Map<String, Object> ctx);
        }
        FlowSpec bp = FlowSpec.define("xib-wire", 1, Map.class, TwoSteps.class, (f, s) -> f
                .execution(ExecutionMode.LOCAL_ASYNC)
                .thenApply(s::x)
                .thenApply(s::y));
        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            client.register(bp);
            client.start(bp, Map.of());
            client.start(bp, Map.of());
            List<com.wiggle.core.TaskActivation> claimed =
                    client.poll("wire-w", bp.definition().queues(), 10, 30_000, 0).tasks();
            assertEquals(2, claimed.size());

            List<WiggleClient.RunSubmission> runs = new ArrayList<>();
            for (com.wiggle.core.TaskActivation t : claimed) {
                String yNode = bp.definition().node(t.nodeId()).next();
                runs.add(new WiggleClient.RunSubmission(t.taskId(), "wire-w", List.of(
                        new WiggleClient.StepReport(t.nodeId(), Map.of("x", 1L), null),
                        new WiggleClient.StepReport(yNode, Map.of("x", 1L, "y", 2L), null)), true));
            }
            Map<String, WiggleClient.RunOutcome> results = client.advanceMany(runs);

            assertEquals(2, results.size());
            for (com.wiggle.core.TaskActivation t : claimed) {
                WiggleClient.RunOutcome r = results.get(t.taskId());
                assertEquals(true, r.ok(), String.valueOf(r));
                assertEquals("COMPLETED", r.outcome().instanceStatus());
            }
        }
    }
}
