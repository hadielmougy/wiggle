package com.wiggle.tests;

import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Branch;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.client.WiggleClient;
import com.wiggle.client.worker.Worker;
import com.wiggle.client.worker.WorkerOptions;
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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The local-execution handback boundaries that linear pipelines never reach: a fork mid-chain,
 * a false gate, and a step routed to a queue this worker does not serve (worker specialization
 * via {@link WorkerOptions#withQueues}).
 */
class LocalBoundaryTest {

    private static Map<String, Object> put(Map<String, Object> ctx, String k, Object v) {
        Map<String, Object> n = new LinkedHashMap<>(ctx);
        n.put(k, v);
        return n;
    }

    private static ServerConfig config() {
        return new ServerConfig(0, "lb-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    @Test @DisplayName("a fork mid-chain hands back and still merges both branches (all local modes)")
    void forkHandsBack() throws Exception {
        for (ExecutionMode mode : new ExecutionMode[]{ExecutionMode.LOCAL_SYNC, ExecutionMode.LOCAL_ASYNC}) {
            Map<String, AtomicInteger> runs = new ConcurrentHashMap<>();
            Blueprint<Map<String, Object>> bp = Workflow.define("lb-fork")
                    .execution(mode)
                    .step("seed", ctx -> counted(runs, "seed", put(ctx, "seeded", true)))
                    .step("prep", ctx -> counted(runs, "prep", put(ctx, "prepped", true)))
                    .fork(
                            Branch.of("left", s -> s.step("l1", ctx -> counted(runs, "l1", put(ctx, "left", "L")))
                                                    .step("l2", ctx -> counted(runs, "l2", put(ctx, "left2", "L2")))),
                            Branch.of("right", s -> s.step("r1", ctx -> counted(runs, "r1", put(ctx, "right", "R")))))
                    .step("after", ctx -> counted(runs, "after", put(ctx, "joined", true)))
                    .build();

            try (WiggleServer server = new WiggleServer(config()).start();
                 WiggleClient client = new WiggleClient(server.baseUrl());
                 Worker w = new Worker(client, "lb-" + Ids.next("x")).register(bp)) {
                w.start();
                InstanceView v = client.awaitCompletion(client.start(bp, Map.of()), Duration.ofSeconds(20));
                assertEquals("COMPLETED", v.status(), mode + " status");
                Map<String, Object> ctx = Json.asObject(v.context());
                assertEquals("L", ctx.get("left"), mode + " left branch");
                assertEquals("L2", ctx.get("left2"), mode + " left branch tail");
                assertEquals("R", ctx.get("right"), mode + " right branch");
                assertEquals(true, ctx.get("joined"), mode + " continuation ran");
                for (String step : Set.of("seed", "prep", "l1", "l2", "r1", "after")) {
                    assertEquals(1, runs.get(step).get(), mode + " step '" + step + "' ran exactly once");
                }
            }
        }
    }

    @Test @DisplayName("a false gate mid-chain ends the instance as gated (LOCAL_SYNC)")
    void gateFalseHandsBack() throws Exception {
        AtomicInteger downstream = new AtomicInteger();
        Blueprint<Map<String, Object>> bp = Workflow.define("lb-gate")
                .execution(ExecutionMode.LOCAL_SYNC)
                .step("seed", ctx -> put(ctx, "keep", false))
                .gate("keep", ctx -> Boolean.TRUE.equals(ctx.get("keep")))
                .step("never", ctx -> {
                    downstream.incrementAndGet();
                    return ctx;
                })
                .build();

        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker w = new Worker(client, "lb-" + Ids.next("x")).register(bp)) {
            w.start();
            InstanceView v = client.awaitCompletion(client.start(bp, Map.of()), Duration.ofSeconds(20));
            assertEquals("COMPLETED", v.status());
            assertEquals("gated:keep", v.terminationReason());
            assertEquals(0, downstream.get(), "downstream must not run");
        }
    }

    @Test @DisplayName("a per-step queue routes steps to a specialized worker (SERVER mode)")
    void queueRoutingServer() throws Exception {
        runQueueSplit(ExecutionMode.SERVER);
    }

    @Test @DisplayName("a step on an unserved queue hands the local chain back to the other worker (LOCAL_SYNC)")
    void otherQueueHandsBackLocal() throws Exception {
        runQueueSplit(ExecutionMode.LOCAL_SYNC);
    }

    /**
     * Two specialized workers: "general" serves the default queue, "special" serves only the
     * dedicated one. Each worker registers its own blueprint whose handlers close over the
     * worker's label -- the topology (and so the version) is identical, but the recorded label
     * proves which worker executed each step: the routing itself, and -- in LOCAL_SYNC -- the
     * OTHER_QUEUE handback mid-chain.
     */
    private void runQueueSplit(ExecutionMode mode) throws Exception {
        Map<String, String> ranOn = new ConcurrentHashMap<>();
        Blueprint<Map<String, Object>> generalBp = queueSplitBlueprint(mode, "general", ranOn);
        Blueprint<Map<String, Object>> specialBp = queueSplitBlueprint(mode, "special", ranOn);
        assertEquals(generalBp.version(), specialBp.version(), "same topology, same version");

        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker general = new Worker(client, "general",
                     WorkerOptions.defaults().withQueues("lb-queues")).register(generalBp);
             Worker special = new Worker(client, "special",
                     WorkerOptions.defaults().withQueues("special")).register(specialBp)) {
            general.start();
            special.start();
            InstanceView v = client.awaitCompletion(client.start(generalBp, Map.of()), Duration.ofSeconds(20));
            assertEquals("COMPLETED", v.status(), mode + " status");
            assertEquals("general", ranOn.get("a"), mode + ": default-queue step");
            assertEquals("general", ranOn.get("b"), mode + ": default-queue step");
            assertEquals("special", ranOn.get("c"), mode + ": dedicated-queue step");
            assertEquals("general", ranOn.get("d"), mode + ": back on the default queue");
        }
    }

    private static Blueprint<Map<String, Object>> queueSplitBlueprint(
            ExecutionMode mode, String label, Map<String, String> ranOn) {
        return Workflow.define("lb-queues")
                .execution(mode)
                .step("a", ctx -> tag(ranOn, "a", label, ctx))
                .step("b", ctx -> tag(ranOn, "b", label, ctx))
                .step("c", ctx -> tag(ranOn, "c", label, ctx), "special")
                .step("d", ctx -> tag(ranOn, "d", label, ctx))
                .build();
    }

    private static Map<String, Object> counted(Map<String, AtomicInteger> runs, String step, Map<String, Object> ctx) {
        runs.computeIfAbsent(step, k -> new AtomicInteger()).incrementAndGet();
        return ctx;
    }

    private static Map<String, Object> tag(Map<String, String> ranOn, String step, String label, Map<String, Object> ctx) {
        ranOn.put(step, label);
        return ctx;
    }
}
