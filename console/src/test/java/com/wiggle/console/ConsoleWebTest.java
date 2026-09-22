package com.wiggle.console;

import com.wiggle.client.DirectConnection;
import com.wiggle.client.WiggleClient;
import com.wiggle.client.WiggleConnection;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.core.ExecutionMode;
import com.wiggle.core.WorkflowDefinition;
import com.wiggle.server.engine.WorkflowEngine.StepInput;
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
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The console's Tomcat/servlet web tier end to end: the SPA API over HTTP, and the auth filter. */
class ConsoleWebTest {

    static {
        System.setProperty("wiggle.observe.settleMillis", "0");   // observed runs are judged at settle; no grace here
    }

    /** The steps a spec names. A worker binds them by name; nothing here implements them. */
    interface Steps {
        Map<String, Object> work(Map<String, Object> ctx);
        Map<String, Object> more(Map<String, Object> ctx);
    }

    private static FlowSpec wf() {
        return FlowSpec.define("wf", 1, Map.class, Steps.class, (f, s) -> f.thenApply(s::work));
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

    private static HttpResponse<String> post(HttpClient http, String url, String basic) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url)).POST(HttpRequest.BodyPublishers.noBody());
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
                assertEquals("{\"required\":false,\"user\":null,\"role\":\"operator\",\"canWrite\":true}",
                        get(http, base + "/api/auth", null).body(), "open mode = full operator access");

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

    @Test @DisplayName("backlog: /api/backlog reports work no worker can claim, and says so in the summary")
    void backlogCoverageOverHttp() throws Exception {
        try (WiggleServer server = new WiggleServer(config()).start();
             DirectConnection conn = WiggleConnection.direct(server.baseUrl())) {
            WiggleClient c = conn.client();
            // no worker is ever started here, so this token is dispatchable and unclaimable -- which is
            // exactly the state the rest of the console cannot show: the instance reads RUNNING.
            FlowSpec stranded = FlowSpec.define("stranded", 1, Map.class, Steps.class,
                    (f, s) -> f.thenApply(s::work, "nobody-polls-this"));
            c.register(stranded);
            String id = c.start(stranded, Map.of());

            ConsoleAuth auth = new ConsoleAuth("admin", null, false);
            try (ConsoleServer console = new ConsoleServer(new GrpcDashboardData(new ConsoleBackend.Direct(conn)),
                    auth, 0, Tls.Options.DISABLED).start()) {
                String base = "http://localhost:" + console.port();
                HttpClient http = HttpClient.newHttpClient();

                String body = awaitBody(http, base + "/api/backlog", "nobody-polls-this");
                assertTrue(body.contains("\"queue\":\"nobody-polls-this\""), "the orphan queue is listed");
                assertTrue(body.contains("\"covered\":false"), "and reported as uncovered");
                assertTrue(body.contains("\"version\":" + stranded.version()),
                        "attributed to the version that produced it");
                assertTrue(body.contains("\"uncoveredSlices\":1"), "the summary counts it");
                assertTrue(body.contains("\"strandedTasks\":1"), "along with how many tasks are stuck");
                assertTrue(body.contains("\"livePollers\":0"), "with nothing polling at all");

                assertTrue(get(http, base + "/api/instances/" + id, null).body().contains("RUNNING"),
                        "while the instance itself still looks healthy -- the point of the view");
            }
        }
    }

    /** The token is written asynchronously by the engine, so poll briefly for it rather than sleeping. */
    private static String awaitBody(HttpClient http, String url, String expect) throws Exception {
        long deadline = System.currentTimeMillis() + 10_000;
        String last = "";
        while (System.currentTimeMillis() < deadline) {
            last = get(http, url, null).body();
            if (last.contains(expect)) return last;
            Thread.sleep(100);
        }
        throw new AssertionError("never saw " + expect + " in " + url + "; last body: " + last);
    }

    @Test @DisplayName("authorization: a read-only viewer can read but is 403'd on mutating calls")
    void viewerIsReadOnly() throws Exception {
        try (WiggleServer server = new WiggleServer(config()).start();
             DirectConnection conn = WiggleConnection.direct(server.baseUrl())) {
            WiggleClient c = conn.client();
            c.register(wf());
            String id = c.start("wf", Map.of(), null, null);

            ConsoleAuth auth = new ConsoleAuth("admin", "op-pass", "viewer", "view-pass", false);
            try (ConsoleServer console = new ConsoleServer(new GrpcDashboardData(new ConsoleBackend.Direct(conn)),
                    auth, 0, Tls.Options.DISABLED).start()) {
                String base = "http://localhost:" + console.port();
                HttpClient http = HttpClient.newHttpClient();
                String cancel = base + "/api/instances/" + id + "/cancel";

                // viewer: reads work, /api/auth advertises read-only, but a mutating POST is 403
                assertEquals(200, get(http, base + "/api/instances", "viewer:view-pass").statusCode(), "viewer reads");
                assertTrue(get(http, base + "/api/auth", "viewer:view-pass").body()
                        .contains("\"role\":\"viewer\"") , "role advertised");
                assertTrue(get(http, base + "/api/auth", "viewer:view-pass").body()
                        .contains("\"canWrite\":false"), "viewer can't write");
                assertEquals(403, post(http, cancel, "viewer:view-pass").statusCode(), "viewer cancel is forbidden");
                assertTrue(get(http, base + "/api/instances/" + id, "viewer:view-pass").body().contains("RUNNING"),
                        "instance untouched by the rejected cancel");

                // operator: same call succeeds
                assertTrue(get(http, base + "/api/auth", "admin:op-pass").body().contains("\"canWrite\":true"),
                        "operator can write");
                assertEquals(200, post(http, cancel, "admin:op-pass").statusCode(), "operator cancel works");
                assertTrue(get(http, base + "/api/instances/" + id, "admin:op-pass").body().contains("CANCELLED"),
                        "operator cancel took");

                // wrong password authenticates as neither -> 401
                assertEquals(401, get(http, base + "/api/instances", "viewer:nope").statusCode(), "bad creds rejected");
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

    /** An observed workflow: a spec with no mode, stamped OBSERVED as the observe module does. */
    private static FlowSpec observed() {
        FlowSpec spec = FlowSpec.define("obs", 1, Map.class, Steps.class, (f, s) -> f.thenApply(s::work).thenApply(s::more));
        WorkflowDefinition d = spec.definition();
        return new FlowSpec(new WorkflowDefinition(d.name(), d.version(), d.startNode(), d.nodes(), d.queues(),
                ExecutionMode.OBSERVED, d.checkpoints()));
    }

    @Test @DisplayName("performance: /api/stats ranks steps by p95 and /api/anomalies lists departures from the topology")
    void statsAndAnomaliesOverHttp() throws Exception {
        try (WiggleServer server = new WiggleServer(config()).start();
             DirectConnection conn = WiggleConnection.direct(server.baseUrl())) {
            FlowSpec obs = observed();
            conn.client().register(obs);
            String a = obs.definition().startNode();
            String b = obs.definition().node(a).next();
            long t0 = 1_700_000_000_000L;
            // one clean run: work 10ms, more 40ms
            server.engine().observe("obs", null, null, "run-1", "app", List.of(
                    new StepInput(a, null, null, null, t0, t0 + 10),
                    new StepInput(b, null, null, null, t0 + 10, t0 + 50)), true);
            // one run that reports 'more' where 'work' was due
            server.engine().observe("obs", null, null, "run-2", "app", List.of(
                    new StepInput(b, null, null, null, t0, t0 + 30)), true);
            server.engine().settleObservedRuns(10);   // the leader's sweep, run by hand: judges both runs

            ConsoleAuth auth = new ConsoleAuth("admin", null, false);
            try (ConsoleServer console = new ConsoleServer(new GrpcDashboardData(new ConsoleBackend.Direct(conn)),
                    auth, 0, Tls.Options.DISABLED).start()) {
                String base = "http://localhost:" + console.port();
                HttpClient http = HttpClient.newHttpClient();

                String stats = get(http, base + "/api/stats?workflow=obs", null).body();
                assertTrue(stats.contains("\"workflow\":\"obs\""), stats);
                int more = stats.indexOf("\"name\":\"more\""), work = stats.indexOf("\"name\":\"work\"");
                assertTrue(more >= 0 && work >= 0, "both steps have stats: " + stats);
                assertTrue(more < work, "slowest p95 first: " + stats);
                assertTrue(stats.contains("\"p95Millis\":40"), "more's p95 over its two runs: " + stats);
                assertTrue(stats.contains("\"count\":2"), "more ran twice: " + stats);
                assertEquals(400, get(http, base + "/api/stats", null).statusCode(), "a workflow is required");

                String anomalies = get(http, base + "/api/anomalies?workflow=obs", null).body();
                assertTrue(anomalies.contains("\"kind\":\"OUT_OF_ORDER\""), anomalies);
                assertTrue(anomalies.contains("\"expectedNode\":\"" + a + "\""), anomalies);
                assertTrue(anomalies.contains("\"reportedNode\":\"" + b + "\""), anomalies);
                assertEquals("{\"anomalies\":[]}", get(http, base + "/api/anomalies?workflow=other", null).body(),
                        "filtered by workflow");
            }
        }
    }
}
