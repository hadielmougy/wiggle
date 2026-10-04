package com.wiggle.console;

import com.wiggle.core.Tls;
import com.wiggle.server.engine.EngineException;
import com.wiggle.server.store.StorageException;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The portal is started from the server's environment, on its own port, or not at all. */
class PortalTest {

    private static ServerConfig config() {
        return new ServerConfig(0, "portal-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    @Test @DisplayName("an unset or zero WIGGLE_PORTAL_PORT serves no portal")
    void offByDefault() throws Exception {
        try (WiggleServer server = new WiggleServer(config()).start()) {
            assertTrue(Portal.fromEnvironment(server, Tls.Options.DISABLED, Map.of()).isEmpty());
            assertTrue(Portal.fromEnvironment(server, Tls.Options.DISABLED, Map.of(Portal.PORT_ENV, "0")).isEmpty());
            assertThrows(IllegalArgumentException.class,
                    () -> Portal.fromEnvironment(server, Tls.Options.DISABLED, Map.of(Portal.PORT_ENV, "web")));
        }
    }

    @Test @DisplayName("a set port serves the portal from the server, separate from the gRPC port, behind its login")
    void servesFromTheServer() throws Exception {
        Path users = Files.createTempFile("portal-users", ".json");
        Files.delete(users);
        int port;
        try (java.net.ServerSocket s = new java.net.ServerSocket(0)) {
            port = s.getLocalPort();
        }
        try (WiggleServer server = new WiggleServer(config()).start();
             Portal portal = Portal.fromEnvironment(server, Tls.Options.DISABLED, Map.of(
                     Portal.PORT_ENV, String.valueOf(port),
                     "WIGGLE_DASHBOARD_PASSWORD", "s3cret",
                     "WIGGLE_CONSOLE_USERS_FILE", users.toString())).orElseThrow()) {
            assertEquals(port, portal.port());
            assertTrue(port != server.port(), "the portal is not on the gRPC port");
            HttpClient http = HttpClient.newHttpClient();
            String base = "http://localhost:" + port;
            assertEquals(200, http.send(HttpRequest.newBuilder(URI.create(base + "/healthz")).build(),
                    HttpResponse.BodyHandlers.ofString()).statusCode());
            assertEquals(401, http.send(HttpRequest.newBuilder(URI.create(base + "/api/cluster")).build(),
                    HttpResponse.BodyHandlers.ofString()).statusCode(), "a password set means a login is required");
        } finally {
            Files.deleteIfExists(users);
        }
    }

    /** A backend whose every call throws {@code failure}. */
    private static DashboardData failing(RuntimeException failure) {
        return (DashboardData) Proxy.newProxyInstance(DashboardData.class.getClassLoader(),
                new Class<?>[] {DashboardData.class}, (proxy, method, args) -> { throw failure; });
    }

    private static HttpResponse<String> cluster(DashboardData data) throws Exception {
        try (ConsoleServer http = new ConsoleServer(data, new ConsoleAuth("admin", null, false), 0,
                Tls.Options.DISABLED).start()) {
            return HttpClient.newHttpClient().send(HttpRequest.newBuilder(
                    URI.create("http://localhost:" + http.port() + "/api/cluster")).build(),
                    HttpResponse.BodyHandlers.ofString());
        }
    }

    @Test @DisplayName("a failed call answers with the engine's status, or 503/500 with a reference and no cause")
    void errorsAreMappedWithoutLeakingCauses() throws Exception {
        assertEquals(409, cluster(failing(EngineException.conflict("already finished"))).statusCode());

        HttpResponse<String> unavailable = cluster(failing(new StorageException("FATAL: host db-7.internal",
                null, StorageException.Classification.TRANSIENT)));
        assertEquals(503, unavailable.statusCode());
        assertFalse(unavailable.body().contains("db-7"), unavailable.body());
        assertTrue(unavailable.body().contains("ref "), unavailable.body());

        HttpResponse<String> internal = cluster(failing(new RuntimeException("relation wf_token does not exist")));
        assertEquals(500, internal.statusCode());
        assertFalse(internal.body().contains("wf_token"), internal.body());
    }
}
