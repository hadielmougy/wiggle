package com.wiggle.server.api;

import com.wiggle.core.wire.ControlPlane;
import com.wiggle.postgres.PostgresStorageFactory;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import com.wiggle.testfixtures.TestPorts;
import com.wiggle.tests.RawApi;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The API's HTTP binding: paths, methods, content types and the error body (WGL-API-001, WGL-API-003). */
class HttpApiTest {

    private static ServerConfig config() {
        return new ServerConfig(TestPorts.free(), "http-api", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0);
    }

    @Test @DisplayName("a request is answered in JSON; an empty body is an empty message")
    void json() throws Exception {
        try (WiggleServer server = new WiggleServer(config(), new PostgresStorageFactory()).start()) {
            RawApi.Answer health = RawApi.callJson(server.baseUrl(), ControlPlane.HEALTH_CHECK, "");
            assertEquals(200, health.status());
            assertEquals("UP", health.msg().str("status"));
            assertTrue(health.msg().has("leader"), "false and zero values are written, not left out");

            RawApi.Answer started = RawApi.callJson(server.baseUrl(), ControlPlane.START_INSTANCE,
                    "{\"workflow\": \"nope\", \"context\": {\"a\": 1}}");
            assertEquals(404, started.status());
            assertEquals("NOT_FOUND", started.code());
        }
    }

    @Test @DisplayName("an unknown method is 404, any verb but POST 405, a malformed body 400")
    void routing() throws Exception {
        try (WiggleServer server = new WiggleServer(config(), new PostgresStorageFactory()).start();
             HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()) {
            String base = "http://" + server.baseUrl();
            HttpResponse<String> unknown = http.send(HttpRequest.newBuilder(URI.create(base + "/v1/Nope"))
                    .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
            assertEquals(404, unknown.statusCode());

            HttpResponse<String> get = http.send(HttpRequest.newBuilder(URI.create(base + "/v1/HealthCheck")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(405, get.statusCode());
            assertEquals("POST", get.headers().firstValue("Allow").orElse(null));

            RawApi.Answer malformed = RawApi.callJson(server.baseUrl(), ControlPlane.START_INSTANCE, "{not json");
            assertEquals(400, malformed.status());
            assertEquals("INVALID_ARGUMENT", malformed.code());
            assertTrue(malformed.message().startsWith("malformed request"), malformed.message());
        }
    }

    @Test @DisplayName("a field of the wrong type, or a oneof with two members set, is 400")
    void fieldChecks() throws Exception {
        try (WiggleServer server = new WiggleServer(config(), new PostgresStorageFactory()).start()) {
            RawApi.Answer wrongType = RawApi.callJson(server.baseUrl(), ControlPlane.LIST_INSTANCES, "{\"limit\": \"ten\"}");
            assertEquals(400, wrongType.status());
            assertTrue(wrongType.message().contains("'limit'"), wrongType.message());

            RawApi.Answer notAnObject = RawApi.callJson(server.baseUrl(), ControlPlane.LIST_INSTANCES, "[1]");
            assertEquals(400, notAnObject.status());

            RawApi.Answer both = RawApi.callJson(server.baseUrl(), ControlPlane.CREATE_SCHEDULE,
                    "{\"workflow\": \"w\", \"everyMillis\": 1000, \"cron\": \"* * * * *\"}");
            assertEquals(400, both.status());
            assertTrue(both.message().contains("both"), both.message());
        }
    }

    @Test @DisplayName("a request body over the limit is refused with 413")
    void tooLarge() throws Exception {
        try (WiggleServer server = new WiggleServer(config(), new PostgresStorageFactory()).start();
             HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()) {
            byte[] body = new byte[ApiServlet.MAX_BODY_BYTES + 1];
            HttpResponse<String> resp = http.send(HttpRequest.newBuilder(URI.create("http://" + server.baseUrl()
                            + ControlPlane.START_INSTANCE.path()))
                    .header("Content-Type", ControlPlane.JSON)
                    .POST(HttpRequest.BodyPublishers.ofByteArray(body)).build(), HttpResponse.BodyHandlers.ofString());
            assertEquals(413, resp.statusCode());
        }
    }
}
