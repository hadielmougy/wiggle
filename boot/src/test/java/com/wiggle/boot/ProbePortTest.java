package com.wiggle.boot;

import com.wiggle.testfixtures.TestPorts;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Actuator's health on the probe port, {@code WIGGLE_DASHBOARD_PORT}, at {@code /healthz}. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
class ProbePortTest {

    private static final int API = TestPorts.free();
    private static final int PROBE = TestPorts.free();

    @DynamicPropertySource
    static void settings(DynamicPropertyRegistry registry) {
        registry.add("WIGGLE_PORT", () -> API);
        registry.add("WIGGLE_PORTAL_PORT", () -> 0);
        registry.add("WIGGLE_DASHBOARD_PORT", () -> PROBE);
    }

    private static HttpResponse<String> get(HttpClient http, int port, String path) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test @DisplayName("the probe port answers /healthz, /healthz/liveness and /healthz/readiness, and the API port none of them")
    void healthOnTheProbePort() throws Exception {
        try (HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()) {
            HttpResponse<String> health = get(http, PROBE, "/healthz");
            assertEquals(200, health.statusCode());
            assertTrue(health.body().contains("\"UP\""), health.body());
            assertEquals(200, get(http, PROBE, "/healthz/liveness").statusCode());
            assertEquals(200, get(http, PROBE, "/healthz/readiness").statusCode());
            assertEquals(404, get(http, API, "/healthz").statusCode());
        }
    }
}
