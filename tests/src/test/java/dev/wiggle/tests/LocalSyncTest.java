package dev.wiggle.tests;

import dev.wiggle.client.dsl.Blueprint;
import dev.wiggle.client.dsl.Workflow;
import dev.wiggle.client.worker.WiggleClient;
import dev.wiggle.client.worker.Worker;
import dev.wiggle.client.worker.WorkerOptions;
import dev.wiggle.core.ExecutionMode;
import dev.wiggle.core.Ids;
import dev.wiggle.core.InstanceView;
import dev.wiggle.core.Json;
import dev.wiggle.core.TaskActivation;
import dev.wiggle.server.ServerConfig;
import dev.wiggle.server.WiggleServer;
import dev.wiggle.server.engine.DefinitionRegistry;
import dev.wiggle.server.engine.WorkflowEngine;
import dev.wiggle.server.store.InMemoryStorage;
import dev.wiggle.server.store.Storage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** LOCAL_SYNC execution: identical results to SERVER, and consecutive steps chain on one worker. */
class LocalSyncTest {

    private static Map<String, Object> put(Map<String, Object> ctx, String k, Object v) {
        Map<String, Object> n = new LinkedHashMap<>(ctx);
        n.put(k, v);
        return n;
    }

    private static ServerConfig config() {
        return new ServerConfig(0, "ls-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    /** A five-step linear pipeline; each step's value depends on the previous. */
    private static Blueprint<Map<String, Object>> linear(ExecutionMode mode, AtomicInteger runs) {
        return Workflow.defineJson("ls-linear")
                .execution(mode)
                .step("a", ctx -> { runs.incrementAndGet(); return put(ctx, "a", 1L); })
                .step("b", ctx -> { runs.incrementAndGet(); return put(ctx, "b", (Long) ctx.get("a") + 1); })
                .gate("keep", ctx -> { runs.incrementAndGet(); return (Long) ctx.get("b") > 0; })
                .step("c", ctx -> { runs.incrementAndGet(); return put(ctx, "c", (Long) ctx.get("b") + 1); })
                .step("d", ctx -> { runs.incrementAndGet(); return put(ctx, "d", (Long) ctx.get("c") + 1); })
                .build();
    }

    @Test @DisplayName("a linear pipeline yields the same context under every execution mode")
    void sameResultAsServer() throws Exception {
        for (ExecutionMode mode : new ExecutionMode[]{
                ExecutionMode.SERVER, ExecutionMode.LOCAL_SYNC, ExecutionMode.LOCAL_ASYNC}) {
            AtomicInteger runs = new AtomicInteger();
            Blueprint<Map<String, Object>> bp = linear(mode, runs);
            try (WiggleServer server = new WiggleServer(config()).start();
                 WiggleClient client = new WiggleClient(server.baseUrl());
                 Worker w = new Worker(client, "w-" + Ids.next("x"),
                         WorkerOptions.defaults().withConcurrency(4)).register(bp)) {
                w.start();
                InstanceView v = client.awaitCompletion(client.start(bp, Map.of()), Duration.ofSeconds(20));
                assertEquals("COMPLETED", v.status(), mode + " status");
                Map<String, Object> ctx = Json.asObject(v.context());
                assertEquals(1L, ctx.get("a"), mode + " a");
                assertEquals(2L, ctx.get("b"), mode + " b");
                assertEquals(3L, ctx.get("c"), mode + " c");
                assertEquals(4L, ctx.get("d"), mode + " d");
                assertEquals(5, runs.get(), mode + " every step ran exactly once");
            }
        }
    }

    @Test @DisplayName("a chained continuation is leased to the same worker, not offered to others")
    void continuationStaysOnTheWorker() {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            DefinitionRegistry registry = new DefinitionRegistry(storage);
            WorkflowEngine engine = new WorkflowEngine(storage, registry, 30_000);
            Blueprint<Map<String, Object>> bp = linear(ExecutionMode.LOCAL_SYNC, new AtomicInteger());
            registry.register(bp.definition());
            Set<String> queues = bp.definition().queues();

            String id = engine.start(bp.name(), bp.version(), Map.of(), null);
            assertNotNull(id);

            // Worker w1 claims the first step and sees the resolved mode.
            List<TaskActivation> claimed = engine.poll("w1", queues, 10, null);
            assertEquals(1, claimed.size());
            TaskActivation first = claimed.get(0);
            assertEquals(ExecutionMode.LOCAL_SYNC, first.executionMode(), "mode stamped on the activation");

            // w1 reports step 'a' as non-final; the continuation ('b') is leased straight back to w1.
            WorkflowEngine.AdvanceOutcome out = engine.advanceRun(first.taskId(), "w1",
                    List.of(new WorkflowEngine.StepInput(first.nodeId(), Map.of("a", 1L), null)), false);
            assertEquals("RUNNING", out.instanceStatus(), "instance still running");
            assertNotNull(out.nextTaskId(), "a continuation token was leased back");

            // A different worker polling the same queue gets nothing -- the chain is owned by w1.
            assertTrue(engine.poll("w2", queues, 10, null).isEmpty(),
                    "the continuation must not be offered to another worker");
        }
    }

    @Test @DisplayName("LOCAL_ASYNC: a whole run is applied atomically in one AdvanceRun batch")
    void batchAppliesAtomically() {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            DefinitionRegistry registry = new DefinitionRegistry(storage);
            WorkflowEngine engine = new WorkflowEngine(storage, registry, 30_000);
            Blueprint<Map<String, Object>> bp = Workflow.defineJson("async-batch")
                    .execution(ExecutionMode.LOCAL_ASYNC)
                    .step("x", ctx -> put(ctx, "x", 1L))
                    .step("y", ctx -> put(ctx, "y", 2L))
                    .build();
            registry.register(bp.definition());
            Set<String> queues = bp.definition().queues();

            String id = engine.start(bp.name(), bp.version(), Map.of(), null);
            TaskActivation first = engine.poll("w1", queues, 10, null).get(0);
            String xNode = first.nodeId();
            String yNode = bp.definition().node(xNode).next();

            // The worker buffered both steps and flushes them in a single final batch.
            WorkflowEngine.AdvanceOutcome out = engine.advanceRun(first.taskId(), "w1", List.of(
                    new WorkflowEngine.StepInput(xNode, Map.of("x", 1L), null),
                    new WorkflowEngine.StepInput(yNode, Map.of("y", 2L), null)), true);

            assertEquals("COMPLETED", out.instanceStatus(), "the batch drove the instance to completion");
            Map<String, Object> ctx = Json.asObject(engine.instance(id).orElseThrow().context());
            assertEquals(1L, ctx.get("x"));
            assertEquals(2L, ctx.get("y"));
            assertTrue(engine.poll("w2", queues, 10, null).isEmpty(), "nothing left to dispatch");
        }
    }
}
