package dev.wiggle.tests;

import dev.wiggle.client.dsl.Blueprint;
import dev.wiggle.client.dsl.Workflow;
import dev.wiggle.client.worker.WiggleClient;
import dev.wiggle.client.worker.Worker;
import dev.wiggle.core.InstanceView;
import dev.wiggle.core.Json;
import dev.wiggle.server.ServerConfig;
import dev.wiggle.server.WiggleServer;
import dev.wiggle.server.store.Rows.Token;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** User tasks: park until completed out of band, with an optional deadline that escalates or fails. */
class UserTaskTest {

    private static Map<String, Object> put(Map<String, Object> ctx, String k, Object v) {
        Map<String, Object> n = new LinkedHashMap<>(ctx);
        n.put(k, v);
        return n;
    }

    private static ServerConfig config(int dashboardPort) {
        return new ServerConfig(0, "ut-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(300), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, dashboardPort);
    }

    private static List<Token> awaitPending(WiggleServer server, int expected) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) {
            List<Token> p = server.engine().pendingUserTasks(50);
            if (p.size() >= expected) return p;
            Thread.sleep(20);
        }
        return server.engine().pendingUserTasks(50);
    }

    @Test @DisplayName("parks until completed out of band, then merges the result and advances")
    void completeExternally() throws Exception {
        Blueprint<Map<String, Object>> bp = Workflow.defineJson("ut-approve")
                .userTask("approve")
                .step("after", ctx -> put(ctx, "advanced", true))
                .build();

        try (WiggleServer server = new WiggleServer(config(0)).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker w = new Worker(client, "ut-w").register(bp)) {
            w.start();
            String id = client.start(bp, Map.of("x", 1));

            List<Token> pending = awaitPending(server, 1);
            assertEquals(1, pending.size(), "one task awaiting");
            assertEquals("approve", pending.get(0).activity, "task carries its human name");
            assertEquals("RUNNING", client.instance(id).status(), "instance parks, not terminal");

            server.engine().completeUserTask(pending.get(0).id, Map.of("decision", "approved"));

            InstanceView v = client.awaitCompletion(id, Duration.ofSeconds(20));
            assertEquals("COMPLETED", v.status());
            Map<String, Object> ctx = Json.asObject(v.context());
            assertEquals("approved", ctx.get("decision"), "result merged into context");
            assertEquals(true, ctx.get("advanced"), "flow advanced past the task");
        }
    }

    @Test @DisplayName("a missed deadline runs the escalation branch, then rejoins the flow")
    void deadlineEscalates() throws Exception {
        Blueprint<Map<String, Object>> bp = Workflow.defineJson("ut-escalate")
                .userTask("approve", Duration.ofMillis(250),
                        b -> b.step("escalate", ctx -> put(ctx, "escalated", true)))
                .step("after", ctx -> put(ctx, "advanced", true))
                .build();

        try (WiggleServer server = new WiggleServer(config(0)).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker w = new Worker(client, "ut-w").register(bp)) {
            w.start();
            String id = client.start(bp, Map.of());   // never completed; the deadline fires

            InstanceView v = client.awaitCompletion(id, Duration.ofSeconds(20));
            assertEquals("COMPLETED", v.status());
            Map<String, Object> ctx = Json.asObject(v.context());
            assertEquals(true, ctx.get("escalated"), "escalation branch ran");
            assertEquals(true, ctx.get("advanced"), "flow rejoined after the task");
        }
    }

    @Test @DisplayName("a missed deadline with no escalation fails the instance")
    void deadlineFails() throws Exception {
        Blueprint<Map<String, Object>> bp = Workflow.defineJson("ut-timeout")
                .userTask("approve", Duration.ofMillis(250))
                .step("after", ctx -> ctx)
                .build();

        try (WiggleServer server = new WiggleServer(config(0)).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker w = new Worker(client, "ut-w").register(bp)) {
            w.start();
            String id = client.start(bp, Map.of());

            InstanceView v = client.awaitCompletion(id, Duration.ofSeconds(20));
            assertEquals("FAILED", v.status());
            assertTrue(v.error() != null && v.error().contains("timed out"), "fails with a timeout error");
        }
    }

    @Test @DisplayName("cancelling an instance clears its pending user task")
    void cancelClearsTask() throws Exception {
        Blueprint<Map<String, Object>> bp = Workflow.defineJson("ut-cancel")
                .userTask("approve")
                .step("after", ctx -> ctx)
                .build();

        try (WiggleServer server = new WiggleServer(config(0)).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            client.register(bp);
            String id = client.start(bp, Map.of());
            awaitPending(server, 1);

            client.cancel(id, "changed my mind");
            assertEquals("CANCELLED", client.instance(id).status());
            assertEquals(0, server.engine().pendingUserTasks(50).size(), "no task left awaiting");
        }
    }

    @Test @DisplayName("the dashboard lists a pending task and completes it over HTTP")
    void dashboardCompletesTask() throws Exception {
        int dash;
        try (ServerSocket s = new ServerSocket(0)) { dash = s.getLocalPort(); }

        Blueprint<Map<String, Object>> bp = Workflow.defineJson("ut-http")
                .userTask("sign-off")
                .step("after", ctx -> put(ctx, "advanced", true))
                .build();

        try (WiggleServer server = new WiggleServer(config(dash)).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker w = new Worker(client, "ut-w").register(bp)) {
            w.start();
            String id = client.start(bp, Map.of());
            awaitPending(server, 1);
            String taskId = server.engine().pendingUserTasks(10).get(0).id;

            HttpClient http = HttpClient.newHttpClient();
            String base = "http://localhost:" + server.dashboardPort();

            String tasks = http.send(HttpRequest.newBuilder(URI.create(base + "/api/tasks")).GET().build(),
                    HttpResponse.BodyHandlers.ofString()).body();
            assertTrue(tasks.contains("sign-off"), "task listed on the dashboard");
            assertTrue(tasks.contains(taskId), "with its id");

            HttpResponse<String> done = http.send(HttpRequest.newBuilder(
                            URI.create(base + "/api/tasks/" + taskId + "/complete"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString("{\"decision\":\"signed\"}")).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(200, done.statusCode());

            InstanceView v = client.awaitCompletion(id, Duration.ofSeconds(20));
            assertEquals("COMPLETED", v.status());
            Map<String, Object> ctx = Json.asObject(v.context());
            assertEquals("signed", ctx.get("decision"));
            assertEquals(true, ctx.get("advanced"));
        }
    }
}
