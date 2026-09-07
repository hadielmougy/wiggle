package com.wiggle.console;

import com.wiggle.client.DirectConnection;
import com.wiggle.client.WiggleClient;
import com.wiggle.client.WiggleConnection;
import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.core.Tls;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The console's Tomcat/servlet web tier end to end: the SPA API over HTTP, and the auth filter. */
class ConsoleWebTest {

    private static Blueprint wf() {
        return Workflow.define("wf").step("work").build();
    }

    private static ServerConfig config() {
        return new ServerConfig(0, "console-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    private static HttpResponse<String> get(HttpClient http, String url, String basic) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url)).GET();
        if (basic != null) b.header("Authorization", "Basic " + Base64.getEncoder().encodeToString(basic.getBytes()));
        return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    @Test @DisplayName("unauthenticated: the SPA API serves instances, detail, cancel, cluster, workflows")
    void apiUnauthenticated() throws Exception {
        try (WiggleServer server = new WiggleServer(config()).start();
             DirectConnection conn = WiggleConnection.direct(server.baseUrl())) {
            WiggleClient c = conn.client();
            c.register(wf());
            String a = c.start("wf", Map.of(), null, null);
            String b = c.start("wf", Map.of(), null, null);

            ConsoleAuth auth = new ConsoleAuth("admin", null, false);   // no password -> open
            try (ConsoleServer console = new ConsoleServer(new GrpcDashboardData(new ConsoleBackend.Direct(conn)),
                    auth, 0, Tls.Options.DISABLED).start()) {
                String base = "http://localhost:" + console.port();
                HttpClient http = HttpClient.newHttpClient();

                assertEquals(200, get(http, base + "/healthz", null).statusCode(), "console k8s probe");

                HttpResponse<String> list = get(http, base + "/api/instances", null);
                assertEquals(200, list.statusCode());
                assertTrue(list.body().contains(a) && list.body().contains(b), "both instances listed");

                HttpResponse<String> detail = get(http, base + "/api/instances/" + a, null);
                assertEquals(200, detail.statusCode());
                assertTrue(detail.body().contains("\"tokens\""), "detail carries tokens");

                assertTrue(get(http, base + "/api/cluster", null).body().contains("\"members\""), "cluster");
                assertTrue(get(http, base + "/api/workflows", null).body().contains("wf"), "workflows");
                assertEquals("{\"required\":false,\"user\":null}", get(http, base + "/api/auth", null).body());

                HttpResponse<String> cancelled = http.send(HttpRequest.newBuilder(
                        URI.create(base + "/api/instances/" + a + "/cancel")).POST(HttpRequest.BodyPublishers.noBody())
                        .build(), HttpResponse.BodyHandlers.ofString());
                assertEquals(200, cancelled.statusCode());
                assertTrue(get(http, base + "/api/instances/" + a, null).body().contains("CANCELLED"), "cancel took");
            }
        }
    }

    @Test @DisplayName("search: /api/instances by exact id, and by correlation (business) key")
    void searchByIdAndCorrelation() throws Exception {
        try (WiggleServer server = new WiggleServer(config()).start();
             DirectConnection conn = WiggleConnection.direct(server.baseUrl())) {
            WiggleClient c = conn.client();
            c.register(wf());
            String a = c.start("wf", Map.of(), null, "order-42");
            String b = c.start("wf", Map.of(), null, "order-42");
            String other = c.start("wf", Map.of(), null, "order-99");

            ConsoleAuth auth = new ConsoleAuth("admin", null, false);
            try (ConsoleServer console = new ConsoleServer(new GrpcDashboardData(new ConsoleBackend.Direct(conn)),
                    auth, 0, Tls.Options.DISABLED).start()) {
                String base = "http://localhost:" + console.port();
                HttpClient http = HttpClient.newHttpClient();

                // by correlation key: both order-42 instances, not the order-99 one
                String corr = get(http, base + "/api/instances?correlation=order-42", null).body();
                assertTrue(corr.contains(a) && corr.contains(b), "correlation returns both matches");
                assertTrue(!corr.contains(other), "correlation excludes non-matching key");

                // by exact instance id: just that one
                String byId = get(http, base + "/api/instances?id=" + a, null).body();
                assertTrue(byId.contains(a) && !byId.contains(b), "id returns exactly the one instance");

                // unknown id: 200 with an empty list, not a 404
                HttpResponse<String> miss = get(http, base + "/api/instances?id=wfi_nope", null);
                assertEquals(200, miss.statusCode());
                assertEquals("{\"instances\":[]}", miss.body(), "unknown id -> empty list");
            }
        }
    }

    @Test @DisplayName("with a password: API needs auth, pages redirect to /login, /healthz stays open")
    void authEnforced() throws Exception {
        try (WiggleServer server = new WiggleServer(config()).start();
             DirectConnection conn = WiggleConnection.direct(server.baseUrl())) {
            conn.client().register(wf());
            ConsoleAuth auth = new ConsoleAuth("admin", "s3cret", false);
            try (ConsoleServer console = new ConsoleServer(new GrpcDashboardData(new ConsoleBackend.Direct(conn)),
                    auth, 0, Tls.Options.DISABLED).start()) {
                String base = "http://localhost:" + console.port();
                HttpClient http = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build();

                assertEquals(200, get(http, base + "/healthz", null).statusCode(), "healthz open even with auth on");
                assertEquals(401, get(http, base + "/api/instances", null).statusCode(), "API needs auth");
                assertEquals(200, get(http, base + "/api/instances", "admin:s3cret").statusCode(), "Basic works");
                assertEquals(302, get(http, base + "/", null).statusCode(), "page redirects to login");
                assertEquals(200, get(http, base + "/login", null).statusCode(), "login form open");
                assertTrue(get(http, base + "/api/auth", null).body().contains("\"required\":true"), "auth advertised");
            }
        }
    }
}
