package dev.wiggle.tests;

import dev.wiggle.client.dsl.Blueprint;
import dev.wiggle.client.dsl.Workflow;
import dev.wiggle.client.WiggleClient;
import dev.wiggle.client.WiggleClient.WiggleApiException;
import dev.wiggle.core.TaskActivation;
import dev.wiggle.server.ServerConfig;
import dev.wiggle.server.WiggleServer;
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

    private static ServerConfig config() {
        return new ServerConfig(0, "err-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    @Test @DisplayName("unknown ids and workflows surface as 404 over gRPC")
    void notFound() throws Exception {
        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            WiggleApiException noTask = assertThrows(WiggleApiException.class,
                    () -> client.complete("tok_nope", "w", Map.of()));
            assertEquals(404, noTask.status());
            assertTrue(noTask.isClientError());

            WiggleApiException noWorkflow = assertThrows(WiggleApiException.class,
                    () -> client.start("no-such-workflow", Map.of()));
            assertEquals(404, noWorkflow.status());
        }
    }

    @Test @DisplayName("settling a task without its lease surfaces as 409 over gRPC")
    void conflict() throws Exception {
        Blueprint<Map<String, Object>> bp = Workflow.define("err-conflict")
                .step("work", ctx -> ctx)
                .build();
        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            client.register(bp);
            String id = client.start(bp, Map.of());
            // No worker ever polls, so the token is READY (unleased); settling it must conflict.
            String tokenId = server.engine().tokens(id).get(0).id;
            WiggleApiException e = assertThrows(WiggleApiException.class,
                    () -> client.complete(tokenId, "impostor", Map.of()));
            assertEquals(409, e.status());
            assertTrue(e.getMessage().contains("not RUNNING"), "carries the engine's description");
        }
    }

    @Test @DisplayName("a non-boolean predicate result surfaces as 400 over gRPC")
    void badRequest() throws Exception {
        Blueprint<Map<String, Object>> bp = Workflow.define("err-bad")
                .gate("check", ctx -> true)
                .step("after", ctx -> ctx)
                .build();
        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            client.register(bp);
            client.start(bp, Map.of());
            List<TaskActivation> claimed =
                    client.poll("w", bp.definition().workerQueues(), 1, 30_000, 500).tasks();
            assertEquals(1, claimed.size(), "the gate step is claimable");
            TaskActivation gate = claimed.get(0);

            WiggleApiException e = assertThrows(WiggleApiException.class,
                    () -> client.complete(gate.taskId(), gate.leaseOwner(), Map.of("value", "not-a-boolean")));
            assertEquals(400, e.status());
            assertTrue(e.getMessage().contains("predicate result must be a boolean"));
        }
    }
}
