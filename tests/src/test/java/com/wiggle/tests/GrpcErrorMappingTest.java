package com.wiggle.tests;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.WiggleClient;
import com.wiggle.client.WiggleClient.WiggleApiException;
import com.wiggle.core.TaskActivation;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import com.wiggle.server.store.Rows.Token;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The error contract over the wire: engine exceptions must surface to a real gRPC client with
 * the right status (404 not-found, 409 conflict, 400 bad request), not as opaque internals.
 * This exercises {@code GrpcApi}'s status mapping and the client's reverse mapping together.
 */
class GrpcErrorMappingTest {

    /** The step this spec names; a worker binds it by name. */
    interface OneStep {
        Map<String, Object> work(Map<String, Object> ctx);
        Map<String, Object> after(Map<String, Object> ctx);
        boolean check(Map<String, Object> ctx);
    }

    private static ServerConfig config() {
        return new ServerConfig(TestPorts.free(), "err-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    @Test @DisplayName("unknown ids and workflows surface as 404 over gRPC")
    void notFound() throws Exception {
        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            WiggleApiException noTask = assertThrows(WiggleApiException.class,
                    () -> client.reportSteps("tok_nope", "w",
                            List.of(new WiggleClient.StepReport("any", Map.of(), null)), true));
            assertEquals(404, noTask.status());
            assertTrue(noTask.isClientError());

            WiggleApiException noWorkflow = assertThrows(WiggleApiException.class,
                    () -> client.start("no-such-workflow", Map.of()));
            assertEquals(404, noWorkflow.status());
        }
    }

    @Test @DisplayName("settling a task without its lease surfaces as 409 over gRPC")
    void conflict() throws Exception {
        FlowSpec bp = FlowSpec.define("err-conflict", 1, Map.class, OneStep.class, (f, s) -> f.thenApply(s::work));
        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            client.register(bp);
            String id = client.start(bp, Map.of());
            // No worker ever polls, so the token is READY (unleased); settling it must conflict.
            Token token = server.engine().tokens(id).get(0);
            WiggleApiException e = assertThrows(WiggleApiException.class,
                    () -> client.reportSteps(token.id, "impostor",
                            List.of(new WiggleClient.StepReport(token.nodeId, Map.of(), null)), true));
            assertEquals(409, e.status());
            assertTrue(e.getMessage().contains("not RUNNING"), "carries the engine's description");
        }
    }

    @Test @DisplayName("a predicate step reported without a branch surfaces as 400 over gRPC")
    void badRequest() throws Exception {
        FlowSpec bp = FlowSpec.define("err-bad", 1, Map.class, OneStep.class, (f, s) -> f
                .thenFilter(s::check)
                .thenApply(s::after));
        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            client.register(bp);
            client.start(bp, Map.of());
            List<TaskActivation> claimed =
                    client.poll("w", bp.definition().workerQueues(), 1, 30_000, 500).tasks();
            assertEquals(1, claimed.size(), "the gate step is claimable");
            TaskActivation gate = claimed.get(0);

            WiggleApiException e = assertThrows(WiggleApiException.class,
                    () -> client.reportSteps(gate.taskId(), gate.leaseOwner(),
                            List.of(new WiggleClient.StepReport(gate.nodeId(), Map.of("value", "not-a-boolean"), null)), true));
            assertEquals(400, e.status());
            assertTrue(e.getMessage().contains("predicate result must be a boolean"));
        }
    }
}
