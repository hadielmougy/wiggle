package com.wiggle.tests;

import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A cell serves no web UI (that moved to the console), but keeps a minimal {@code /healthz} on the
 * configured port so Kubernetes can probe the node. Off by default; enabled by a non-zero port.
 */
class HealthzTest {

    @Test @DisplayName("a cell serves /healthz on the configured port for k8s probes")
    void healthzForK8s() throws Exception {
        int port;
        try (ServerSocket s = new ServerSocket(0)) { port = s.getLocalPort(); }

        ServerConfig config = new ServerConfig(0, "health-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, port,
                Duration.ofSeconds(5), Duration.ofSeconds(10));

        try (WiggleServer server = new WiggleServer(config).start()) {
            HttpResponse<String> r = HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/healthz")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(200, r.statusCode());
            assertEquals("ok", r.body());
        }
    }
}
