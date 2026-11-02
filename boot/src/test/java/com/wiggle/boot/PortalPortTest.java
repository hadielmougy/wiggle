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

/** The portal on its own port of the same Tomcat, behind its sign-in, and each port serving only its own paths. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
class PortalPortTest {

    private static final int API = TestPorts.free();
    private static final int PORTAL = TestPorts.free();

    @DynamicPropertySource
    static void settings(DynamicPropertyRegistry registry) {
        registry.add("WIGGLE_PORT", () -> API);
        registry.add("WIGGLE_PORTAL_PORT", () -> PORTAL);
        registry.add("WIGGLE_DASHBOARD_PASSWORD", () -> "s3cret");
    }

    private static int status(HttpClient http, int port, String method, String path) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path));
        request = method.equals("POST") ? request.POST(HttpRequest.BodyPublishers.noBody()) : request.GET();
        return http.send(request.build(), HttpResponse.BodyHandlers.discarding()).statusCode();
    }

    @Test @DisplayName("the portal answers on its port, and a password set means a sign-in is required")
    void portalOnItsPort() throws Exception {
        try (HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()) {
            assertEquals(200, status(http, PORTAL, "GET", "/healthz"));
            assertEquals(401, status(http, PORTAL, "GET", "/api/cluster"));
        }
    }

    @Test @DisplayName("both ports speak HTTP/2 in plaintext (h2c) as well as HTTP/1.1")
    void http2Cleartext() throws Exception {
        try (HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_2).build()) {
            for (int port : new int[] {PORTAL, API}) {
                HttpResponse<Void> response = http.send(HttpRequest.newBuilder(
                        URI.create("http://127.0.0.1:" + port + (port == API ? "/v1/HealthCheck" : "/healthz")))
                        .method(port == API ? "POST" : "GET", HttpRequest.BodyPublishers.noBody()).build(),
                        HttpResponse.BodyHandlers.discarding());
                assertEquals(200, response.statusCode());
                assertEquals(HttpClient.Version.HTTP_2, response.version(), "port " + port);
            }
        }
    }

    @Test @DisplayName("the API is not served on the portal's port, nor the portal on the API's")
    void eachPortItsOwnPaths() throws Exception {
        try (HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()) {
            assertEquals(200, status(http, API, "POST", "/v1/HealthCheck"));
            assertEquals(404, status(http, PORTAL, "POST", "/v1/HealthCheck"));
            assertEquals(404, status(http, API, "GET", "/healthz"));
            assertEquals(404, status(http, API, "GET", "/api/cluster"));
        }
    }
}
