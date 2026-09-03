package com.wiggle.tests;

import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.client.WiggleClient;
import com.wiggle.client.WiggleClient.WiggleApiException;
import com.wiggle.client.worker.Handlers;
import com.wiggle.client.worker.Worker;
import com.wiggle.core.InstanceView;
import com.wiggle.core.Json;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import com.wiggle.server.store.Rows.Token;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Signals: an instance parks on a named wait and is resumed by (instance, signal name) over
 * gRPC or the dashboard, with an optional deadline that escalates or fails.
 */
class SignalTest {

    private static Map<String, Object> put(Map<String, Object> ctx, String k, Object v) {
        Map<String, Object> n = new LinkedHashMap<>(ctx);
        n.put(k, v);
        return n;
    }

    @Handlers("sig-approve")
    static final class ApproveH {
        public Map<String, Object> after(Map<String, Object> c) { return put(c, "advanced", true); }
    }

    @Handlers("sig-wrong")
    static final class WrongH {
        public Map<String, Object> after(Map<String, Object> c) { return c; }
    }

    @Handlers("sig-escalate")
    static final class EscalateH {
        public Map<String, Object> escalate(Map<String, Object> c) { return put(c, "escalated", true); }
        public Map<String, Object> after(Map<String, Object> c) { return put(c, "advanced", true); }
    }

    @Handlers("sig-timeout")
    static final class TimeoutH {
        public Map<String, Object> after(Map<String, Object> c) { return c; }
    }

    @Handlers("sig-http")
    static final class HttpH {
        public Map<String, Object> after(Map<String, Object> c) { return put(c, "advanced", true); }
    }

    private static ServerConfig config(int dashboardPort) {
        return new ServerConfig(0, "sig-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(300), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, dashboardPort,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    private static List<Token> awaitPending(WiggleServer server, int expected) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) {
            List<Token> p = server.engine().pendingSignals(50);
            if (p.size() >= expected) return p;
            Thread.sleep(20);
        }
        return server.engine().pendingSignals(50);
    }

    @Test @DisplayName("an instance parks on a signal wait and resumes when it arrives over gRPC")
    void signalOverGrpc() throws Exception {
        Blueprint bp = Workflow.define("sig-approve")
                .awaitSignal("approval")
                .step("after")
                .build();

        try (WiggleServer server = new WiggleServer(config(0)).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker w = new Worker(client, "sig-w").register(bp).handlers(new ApproveH())) {
            w.start();
            String id = client.start(bp, Map.of("x", 1));

            List<Token> pending = awaitPending(server, 1);
            assertEquals(1, pending.size(), "one signal wait pending");
            assertEquals("approval", pending.get(0).activity, "the wait carries the signal name");
            assertEquals("RUNNING", client.instance(id).status(), "instance parks, not terminal");

            client.signal(id, "approval", Map.of("decision", "approved"));

            InstanceView v = client.awaitCompletion(id, Duration.ofSeconds(20));
            assertEquals("COMPLETED", v.status());
            Map<String, Object> ctx = Json.asObject(v.context());
            assertEquals("approved", ctx.get("decision"), "payload merged into context");
            assertEquals(true, ctx.get("advanced"), "flow advanced past the wait");
        }
    }

    @Test @DisplayName("signalling an instance that is not waiting for that name is a 409")
    void wrongSignalConflicts() throws Exception {
        Blueprint bp = Workflow.define("sig-wrong")
                .awaitSignal("expected")
                .step("after")
                .build();
        try (WiggleServer server = new WiggleServer(config(0)).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker w = new Worker(client, "sig-w2").register(bp).handlers(new WrongH())) {
            w.start();
            String id = client.start(bp, Map.of());
            awaitPending(server, 1);

            WiggleApiException e = assertThrows(WiggleApiException.class,
                    () -> client.signal(id, "unexpected", Map.of()));
            assertEquals(409, e.status());
            assertTrue(e.getMessage().contains("not waiting for signal 'unexpected'"));
        }
    }

    @Test @DisplayName("a missed deadline runs the escalation branch, then rejoins the flow")
    void deadlineEscalates() throws Exception {
        Blueprint bp = Workflow.define("sig-escalate")
                .awaitSignal("approval", Duration.ofMillis(250),
                        b -> b.step("escalate"))
                .step("after")
                .build();

        try (WiggleServer server = new WiggleServer(config(0)).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker w = new Worker(client, "sig-w3").register(bp).handlers(new EscalateH())) {
            w.start();
            String id = client.start(bp, Map.of());   // never signalled; the deadline fires

            InstanceView v = client.awaitCompletion(id, Duration.ofSeconds(20));
            assertEquals("COMPLETED", v.status());
            Map<String, Object> ctx = Json.asObject(v.context());
            assertEquals(true, ctx.get("escalated"), "escalation branch ran");
            assertEquals(true, ctx.get("advanced"), "flow rejoined after the wait");
        }
    }

    @Test @DisplayName("a missed deadline with no escalation fails the instance")
    void deadlineFails() throws Exception {
        Blueprint bp = Workflow.define("sig-timeout")
                .awaitSignal("approval", Duration.ofMillis(250))
                .step("after")
                .build();

        try (WiggleServer server = new WiggleServer(config(0)).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker w = new Worker(client, "sig-w4").register(bp).handlers(new TimeoutH())) {
            w.start();
            InstanceView v = client.awaitCompletion(client.start(bp, Map.of()), Duration.ofSeconds(20));
            assertEquals("FAILED", v.status());
            assertTrue(v.error().contains("timed out"), v.error());
        }
    }

    @Test @DisplayName("the dashboard lists a pending signal and delivers it over HTTP")
    void dashboardDeliversSignal() throws Exception {
        int dash;
        try (ServerSocket s = new ServerSocket(0)) { dash = s.getLocalPort(); }

        Blueprint bp = Workflow.define("sig-http")
                .awaitSignal("sign-off")
                .step("after")
                .build();

        try (WiggleServer server = new WiggleServer(config(dash)).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker w = new Worker(client, "sig-w5").register(bp).handlers(new HttpH())) {
            w.start();
            String id = client.start(bp, Map.of());
            awaitPending(server, 1);

            HttpClient http = HttpClient.newHttpClient();
            String base = "http://localhost:" + server.dashboardPort();

            String signals = http.send(HttpRequest.newBuilder(URI.create(base + "/api/signals")).GET().build(),
                    HttpResponse.BodyHandlers.ofString()).body();
            assertTrue(signals.contains("sign-off"), "signal listed on the dashboard");
            assertTrue(signals.contains(id), "with its instance");

            HttpResponse<String> sent = http.send(HttpRequest.newBuilder(
                            URI.create(base + "/api/instances/" + id + "/signal/sign-off"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString("{\"decision\":\"signed\"}")).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(200, sent.statusCode());

            InstanceView v = client.awaitCompletion(id, Duration.ofSeconds(20));
            assertEquals("COMPLETED", v.status());
            assertEquals("signed", Json.asObject(v.context()).get("decision"));
        }
    }
}
