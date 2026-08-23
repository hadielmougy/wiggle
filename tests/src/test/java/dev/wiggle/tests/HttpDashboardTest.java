package dev.wiggle.tests;

import dev.wiggle.client.dsl.Blueprint;
import dev.wiggle.client.dsl.Workflow;
import dev.wiggle.client.worker.WiggleClient;
import dev.wiggle.client.worker.Worker;
import dev.wiggle.server.ServerConfig;
import dev.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Drives the HTTP dashboard end to end: run an instance, then read it back over the JSON API. */
class HttpDashboardTest {

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) { return s.getLocalPort(); }
    }

    private final HttpClient http = HttpClient.newHttpClient();

    private HttpResponse<String> get(String url) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(url)).GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> post(String url) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(url)).POST(HttpRequest.BodyPublishers.noBody()).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> getAuth(String url, String user, String pass) throws Exception {
        String cred = Base64.getEncoder().encodeToString((user + ":" + pass).getBytes(StandardCharsets.UTF_8));
        return http.send(HttpRequest.newBuilder(URI.create(url)).header("Authorization", "Basic " + cred).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test @DisplayName("the dashboard serves the page and reflects live instances over JSON")
    void dashboardServesLiveData() throws Exception {
        int dash = freePort();
        ServerConfig config = new ServerConfig(0, "dash-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, dash, Duration.ofSeconds(5), Duration.ofSeconds(10));

        Blueprint<Map<String, Object>> done = Workflow.defineJson("dash-done")
                .step("work", ctx -> Map.of("result", "ok")).build();
        Blueprint<Map<String, Object>> waiting = Workflow.defineJson("dash-waiting")
                .sleep("hold", Duration.ofSeconds(30)).step("after", ctx -> ctx).build();

        try (WiggleServer server = new WiggleServer(config).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {

            String base = "http://localhost:" + server.dashboardPort();
            client.register(waiting);

            try (Worker w = new Worker(client, "dash-worker").register(done)) {
                w.start();

                String completedId = client.start(done, Map.of("in", 1));
                assertEquals("COMPLETED", client.awaitCompletion(completedId, Duration.ofSeconds(20)).status());
                String waitingId = client.start(waiting, Map.of());   // parks on the 30s timer, stays RUNNING

                // Static page.
                HttpResponse<String> index = get(base + "/");
                assertEquals(200, index.statusCode());
                assertTrue(index.body().contains("<title>Wiggle"), "serves the dashboard page");

                assertEquals("ok", get(base + "/healthz").body());

                // Workflows and cluster.
                assertTrue(get(base + "/api/workflows").body().contains("dash-done"), "lists workflows");
                assertTrue(get(base + "/api/cluster").body().contains("\"members\""), "reports cluster");

                // Instance list includes the completed one.
                String list = get(base + "/api/instances").body();
                assertTrue(list.contains(completedId), "instance list contains the run");
                assertTrue(list.contains("COMPLETED"), "shows its status");

                // Filter by status.
                assertTrue(get(base + "/api/instances?status=RUNNING").body().contains(waitingId),
                        "status filter finds the waiting instance");

                // Detail returns the instance and its tokens.
                String detail = get(base + "/api/instances/" + completedId).body();
                assertTrue(detail.contains("\"tokens\""), "detail includes tokens");
                assertTrue(detail.contains("\"result\""), "detail includes the merged context");

                // Cancel the waiting instance, then confirm it flipped to CANCELLED.
                assertEquals(200, post(base + "/api/instances/" + waitingId + "/cancel").statusCode());
                assertTrue(get(base + "/api/instances/" + waitingId).body().contains("CANCELLED"),
                        "cancel is reflected");

                // Unknown endpoint and missing instance are clean errors, not hangs.
                assertEquals(404, get(base + "/api/nope").statusCode());
                assertEquals(404, get(base + "/api/instances/does-not-exist").statusCode());
            }
        }
    }

    @Test @DisplayName("serves the ClojureScript SPA bundle and a workflow's graph for the diagram")
    void servesSpaAndWorkflowGraph() throws Exception {
        int dash = freePort();
        ServerConfig config = new ServerConfig(0, "dash-graph-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, dash, Duration.ofSeconds(5), Duration.ofSeconds(10));

        // A graph exercising several node kinds so the diagram endpoint has edges to draw.
        Blueprint<Map<String, Object>> bp = Workflow.defineJson("dash-graph")
                .step("submit", ctx -> ctx)
                .awaitSignal("approval", Duration.ofHours(1),
                        b -> b.step("escalate", ctx -> ctx))
                .step("finish", ctx -> ctx)
                .build();

        try (WiggleServer server = new WiggleServer(config).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            client.register(bp);
            String base = "http://localhost:" + server.dashboardPort();

            // The SPA shell points at the compiled bundle, which is itself served.
            HttpResponse<String> index = get(base + "/");
            assertEquals(200, index.statusCode());
            assertTrue(index.body().contains("id=\"app\""), "serves the SPA shell");
            assertTrue(index.body().contains("/js/app.js"), "shell references the bundle");

            HttpResponse<String> js = get(base + "/js/app.js");
            assertEquals(200, js.statusCode());
            assertTrue(js.headers().firstValue("Content-Type").orElse("").contains("javascript"),
                    "bundle is served as javascript");
            assertTrue(js.body().length() > 10_000, "bundle has real content");

            // An unknown non-API route falls through to index.html for client-side routing.
            assertEquals(200, get(base + "/workflows").statusCode());

            // The workflow graph endpoint returns nodes and edges for the diagram.
            HttpResponse<String> graph = get(base + "/api/workflows/dash-graph");
            assertEquals(200, graph.statusCode());
            String g = graph.body();
            assertTrue(g.contains("\"startNode\""), "graph has a start node");
            assertTrue(g.contains("\"SIGNAL\""), "graph exposes the signal node kind");
            assertTrue(g.contains("\"next\"") && g.contains("\"altNext\""), "graph exposes edges");

            assertEquals(404, get(base + "/api/workflows/no-such-workflow").statusCode());
        }
    }

    @Test @DisplayName("with a password set, the API needs auth, pages redirect to /login, /healthz stays open")
    void securedDashboardRequiresAuth() throws Exception {
        int dash = freePort();
        ServerConfig config = securedConfig(dash);

        try (WiggleServer server = new WiggleServer(config).start()) {
            String base = "http://localhost:" + server.dashboardPort();

            // Health check is exempt -- probes and load balancers reach it without credentials.
            HttpResponse<String> health = get(base + "/healthz");
            assertEquals(200, health.statusCode());
            assertEquals("ok", health.body());

            // An anonymous API call is challenged; an anonymous page is redirected to the login form.
            HttpResponse<String> anonApi = get(base + "/api/instances");
            assertEquals(401, anonApi.statusCode());
            assertTrue(anonApi.headers().firstValue("WWW-Authenticate").orElse("").contains("Basic"));
            HttpResponse<String> anonPage = get(base + "/");
            assertEquals(302, anonPage.statusCode(), "the SPA redirects to login");
            assertEquals("/login", anonPage.headers().firstValue("Location").orElse(""));

            // The login form is always reachable.
            HttpResponse<String> loginForm = get(base + "/login");
            assertEquals(200, loginForm.statusCode());
            assertTrue(loginForm.body().contains("Sign in"), "serves the login page");

            // Basic auth still works for curl/programmatic clients.
            assertEquals(401, getAuth(base + "/api/instances", "admin", "wrong").statusCode());
            HttpResponse<String> viaBasic = getAuth(base + "/api/cluster", "admin", "s3cret");
            assertEquals(200, viaBasic.statusCode());
            assertTrue(viaBasic.body().contains("\"members\""));
        }
    }

    @Test @DisplayName("the login form establishes a session cookie, and /logout revokes it")
    void loginEstablishesSession() throws Exception {
        int dash = freePort();
        try (WiggleServer server = new WiggleServer(securedConfig(dash)).start()) {
            String base = "http://localhost:" + server.dashboardPort();

            // Wrong credentials are rejected, no cookie issued.
            assertEquals(401, postJson(base + "/api/login", "{\"user\":\"admin\",\"password\":\"nope\"}").statusCode());

            // Correct credentials mint a session cookie.
            HttpResponse<String> login = postJson(base + "/api/login", "{\"user\":\"admin\",\"password\":\"s3cret\"}");
            assertEquals(200, login.statusCode());
            String cookie = sessionCookie(login);
            assertTrue(cookie.startsWith("wiggle_session="), "issues a session cookie");
            assertTrue(login.headers().firstValue("Set-Cookie").orElse("").contains("HttpOnly"), "cookie is HttpOnly");

            // The cookie authorises the API and the SPA -- no Basic header needed.
            assertEquals(200, getCookie(base + "/api/cluster", cookie).statusCode());
            assertEquals(200, getCookie(base + "/", cookie).statusCode());

            // Logout revokes the session server-side; the same cookie no longer authorises.
            HttpResponse<String> logout = getCookie(base + "/logout", cookie);
            assertEquals(302, logout.statusCode());
            assertEquals("/login", logout.headers().firstValue("Location").orElse(""));
            assertEquals(401, getCookie(base + "/api/cluster", cookie).statusCode(), "session revoked");
        }
    }

    private static ServerConfig securedConfig(int dashboardPort) {
        return new ServerConfig(0, "dash-secure-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, dashboardPort,
                Duration.ofSeconds(5), Duration.ofSeconds(10), "admin", "s3cret");
    }

    private HttpResponse<String> postJson(String url, String json) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json)).build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> getCookie(String url, String cookie) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(url)).header("Cookie", cookie).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    /** The bare {@code name=value} of the Set-Cookie header, for replaying as a Cookie. */
    private static String sessionCookie(HttpResponse<String> r) {
        String setCookie = r.headers().firstValue("Set-Cookie").orElseThrow();
        return setCookie.split(";", 2)[0];
    }
}
