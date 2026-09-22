package com.wiggle.tests;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.ForFlow;
import com.wiggle.client.worker.Worker;
import com.wiggle.client.worker.WorkerOptions;
import com.wiggle.core.ExecutionMode;
import com.wiggle.core.InstanceView;
import com.wiggle.core.NodeStats;
import com.wiggle.core.NodeKind;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import com.wiggle.server.store.Rows.Token;
import com.wiggle.server.store.Rows.TokenStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Worker-run steps get the same performance model as observed ones: the worker reports the
 * handler's own start and finish with every completion, in every mode, and the server keeps how
 * long the step waited to be claimed, so the Performance view covers every execution mode.
 */
class StepTimingTest {

    interface Steps {
        Map<String, Object> quick(Map<String, Object> ctx);
        Map<String, Object> slow(Map<String, Object> ctx);
        boolean keep(Map<String, Object> ctx);
        Map<String, Object> last(Map<String, Object> ctx);
    }

    @ForFlow("timed")
    static final class H {
        public Map<String, Object> quick(Map<String, Object> c) { return c; }
        public Map<String, Object> slow(Map<String, Object> c) {
            try { Thread.sleep(40); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            return c;
        }
        public boolean keep(Map<String, Object> c) { return true; }
        public Map<String, Object> last(Map<String, Object> c) { return c; }
    }

    private static ServerConfig config() {
        return new ServerConfig(TestPorts.free(), "timing-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    @Test @DisplayName("every worker-run step carries the handler's own times and its queue wait, in every mode")
    void workerStepsAreTimed() throws Exception {
        for (ExecutionMode mode : List.of(ExecutionMode.SERVER, ExecutionMode.LOCAL_SYNC, ExecutionMode.LOCAL_ASYNC)) {
            FlowSpec spec = FlowSpec.define("timed", 1, Map.class, Steps.class, (f, s) -> Modes.in(f, mode)
                    .thenApply(s::quick).thenApply(s::slow).thenFilter(s::keep).thenApply(s::last));
            try (WiggleServer server = new WiggleServer(config()).start();
                 WiggleClient client = new WiggleClient(server.baseUrl());
                 Worker worker = new Worker(client, "w-" + mode, WorkerOptions.defaults().withConcurrency(2))
                         .registerHandler(new H())) {
                client.register(spec);
                worker.start();
                InstanceView v = client.awaitCompletion(client.start(spec, Map.of()), Duration.ofSeconds(20));
                assertEquals("COMPLETED", v.status(), mode.toString());

                List<Token> steps = server.engine().tokens(v.id()).stream()
                        .filter(t -> t.status == TokenStatus.DONE && (t.kind == NodeKind.TASK || t.kind == NodeKind.PREDICATE))
                        .toList();
                assertEquals(4, steps.size(), mode + ": four worker steps settled");
                for (Token t : steps) {
                    assertNotNull(t.startedAt, mode + ": " + t.nodeId + " stamped at claim");
                    assertNotNull(t.finishedAt, mode + ": " + t.nodeId + " stamped at settle");
                    assertTrue(t.finishedAt >= t.startedAt, mode + ": " + t.nodeId);
                }
                if (mode == ExecutionMode.SERVER) {
                    // A local worker chains a step before the server holds its token, so only a
                    // dispatched step was ready before it started; a chained one waited for nothing.
                    assertTrue(steps.stream().allMatch(t -> t.startedAt >= t.availableAt), "claimed after it was ready");
                }
                String slowNode = spec.definition().node(spec.definition().startNode()).next();
                Token slow = steps.stream().filter(t -> t.nodeId.equals(slowNode)).findFirst().orElseThrow();
                assertTrue(slow.finishedAt - slow.startedAt >= 40, mode + ": slow ran 40ms, measured " + (slow.finishedAt - slow.startedAt));
                assertTrue(slow.finishedAt - slow.startedAt < 1_000, mode + ": the handler's own clock, not a round trip or a whole batch");
                Token quick = steps.stream().filter(t -> t.nodeId.equals(spec.definition().startNode())).findFirst().orElseThrow();
                assertTrue(quick.finishedAt - quick.startedAt < 40, mode + ": quick is quick by the handler's clock, not the flush's");

                List<NodeStats> stats = server.engine().stepStats("timed", null, 0, 100);
                assertEquals(4, stats.size(), mode + ": every step has stats");
                assertEquals(slowNode, stats.getFirst().nodeId(), mode + ": slowest p95 first");
                assertTrue(stats.stream().allMatch(n -> n.waitP95Millis() >= 0 && n.waitP50Millis() <= n.waitP95Millis()), mode.toString());
                if (mode == ExecutionMode.SERVER) {
                    assertTrue(stats.stream().allMatch(n -> n.waitP95Millis() < 5_000), "a step waited at most one poll interval, not forever");
                }
            }
        }
    }
}
