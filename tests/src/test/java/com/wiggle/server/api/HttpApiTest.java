package com.wiggle.server.api;

import com.wiggle.boot.test.WiggleBootTest;
import com.wiggle.boot.test.WiggleTestNode;
import com.wiggle.core.wire.ControlPlane;
import com.wiggle.tests.RawApi;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The API's HTTP binding: paths, methods, content types and the error body (WGL-API-001, WGL-API-003). */
@WiggleBootTest
class HttpApiTest {

    @Autowired WiggleTestNode node;

    @Test @DisplayName("a request is answered in JSON; an empty body is an empty message")
    void json() throws Exception {
        RawApi.Answer health = RawApi.callJson(node.baseUrl(), ControlPlane.HEALTH_CHECK, "");
        assertEquals(200, health.status());
        assertEquals("UP", health.msg().str("status"));
        assertTrue(health.msg().has("leader"), "false and zero values are written, not left out");

        RawApi.Answer started = RawApi.callJson(node.baseUrl(), ControlPlane.START_INSTANCE,
                "{\"workflow\": \"nope\", \"context\": {\"a\": 1}}");
        assertEquals(404, started.status());
        assertEquals("NOT_FOUND", started.code());
    }

    @Test @DisplayName("an unknown method is 404, any verb but POST 405, a malformed body 400")
    void routing() throws Exception {
        try (HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()) {
            String base = "http://" + node.baseUrl();
            HttpResponse<String> unknown = http.send(HttpRequest.newBuilder(URI.create(base + "/v1/Nope"))
                    .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
            assertEquals(404, unknown.statusCode());

            HttpResponse<String> get = http.send(HttpRequest.newBuilder(URI.create(base + "/v1/HealthCheck")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(405, get.statusCode());
            assertEquals("POST", get.headers().firstValue("Allow").orElse(null));

            RawApi.Answer malformed = RawApi.callJson(node.baseUrl(), ControlPlane.START_INSTANCE, "{not json");
            assertEquals(400, malformed.status());
            assertEquals("INVALID_ARGUMENT", malformed.code());
            assertTrue(malformed.message().startsWith("malformed request"), malformed.message());
        }
    }

    @Test @DisplayName("a field of the wrong type, or a oneof with two members set, is 400")
    void fieldChecks() throws Exception {
        RawApi.Answer wrongType = RawApi.callJson(node.baseUrl(), ControlPlane.LIST_INSTANCES, "{\"limit\": \"ten\"}");
        assertEquals(400, wrongType.status());
        assertTrue(wrongType.message().contains("'limit'"), wrongType.message());

        RawApi.Answer notAnObject = RawApi.callJson(node.baseUrl(), ControlPlane.LIST_INSTANCES, "[1]");
        assertEquals(400, notAnObject.status());

        RawApi.Answer both = RawApi.callJson(node.baseUrl(), ControlPlane.CREATE_SCHEDULE,
                "{\"workflow\": \"w\", \"everyMillis\": 1000, \"cron\": \"* * * * *\"}");
        assertEquals(400, both.status());
        assertTrue(both.message().contains("both"), both.message());
    }

    @Test @DisplayName("a request body over the limit is refused with 413")
    void tooLarge() throws Exception {
        try (HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()) {
            byte[] body = new byte[ApiServlet.MAX_BODY_BYTES + 1];
            HttpResponse<String> resp = http.send(HttpRequest.newBuilder(URI.create("http://" + node.baseUrl()
                            + ControlPlane.START_INSTANCE.path()))
                    .header("Content-Type", ControlPlane.JSON)
                    .POST(HttpRequest.BodyPublishers.ofByteArray(body)).build(), HttpResponse.BodyHandlers.ofString());
            assertEquals(413, resp.statusCode());
        }
    }
}
