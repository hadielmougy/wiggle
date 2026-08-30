package com.wiggle.server.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;
import com.wiggle.core.InstanceView;
import com.wiggle.core.Tls;
import com.wiggle.server.cluster.ClusterManager;
import com.wiggle.server.engine.WorkflowEngine;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.Rows.ServerNode;
import com.wiggle.server.store.Rows.Token;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import static com.wiggle.server.http.DashboardHttp.emptyToNull;
import static com.wiggle.server.http.DashboardHttp.parseInt;
import static com.wiggle.server.http.DashboardHttp.query;
import static com.wiggle.server.http.DashboardHttp.readJsonBody;
import static com.wiggle.server.http.DashboardHttp.redirect;
import static com.wiggle.server.http.DashboardHttp.requireGet;
import static com.wiggle.server.http.DashboardHttp.sendBytes;
import static com.wiggle.server.http.DashboardHttp.sendError;
import static com.wiggle.server.http.DashboardHttp.sendJson;
import static com.wiggle.server.http.DashboardHttp.sendText;

/**
 * A small, dependency-free read-only web dashboard. It runs on the JDK's built-in
 * {@link HttpServer} and calls the in-process engine directly -- no gRPC hop, no proxy, no
 * build step. A single static page polls the JSON endpoints below.
 *
 * <p>Every node can run its own dashboard; because they share the database, any node's view
 * is the whole system's. The endpoints are read-only except for cancelling an instance.
 *
 * <p><b>Access control is pluggable.</b> Authentication is delegated to a {@link DashboardAuth};
 * the default is {@link PasswordAuth} (a single admin account, by session cookie or HTTP Basic).
 * Every endpoint -- the SPA and the whole JSON API -- is guarded except {@code /healthz} and
 * {@code /api/auth}, which are always open. An unauthenticated browser hitting a page is redirected
 * to {@link DashboardAuth#loginLocation}; an unauthenticated API call gets 401 (with the
 * {@link DashboardAuth#apiChallenge}, if any). The authenticator registers its own login endpoints
 * via {@link DashboardAuth#install}. Supply a custom one (e.g. SSO) with
 * {@code new WiggleServer(config, storageFactory, auth)}.
 */
public final class HttpDashboard implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(HttpDashboard.class.getName());

    private final HttpServer http;
    private final WorkflowEngine engine;
    private final ClusterManager cluster;
    private final DashboardAuth auth;

    public HttpDashboard(WorkflowEngine engine, ClusterManager cluster, int port) throws IOException {
        this(engine, cluster, port, "admin", null, Tls.Options.DISABLED);
    }

    public HttpDashboard(WorkflowEngine engine, ClusterManager cluster, int port,
                         String user, String password) throws IOException {
        this(engine, cluster, port, user, password, Tls.Options.DISABLED);
    }

    /** Uses the built-in {@link PasswordAuth}; use the {@link DashboardAuth} overload to plug in SSO etc. */
    public HttpDashboard(WorkflowEngine engine, ClusterManager cluster, int port,
                         String user, String password, Tls.Options tls) throws IOException {
        this(engine, cluster, port, new PasswordAuth(user, password, tls.hasKeyStore()), tls);
    }

    public HttpDashboard(WorkflowEngine engine, ClusterManager cluster, int port,
                         DashboardAuth auth, Tls.Options tls) throws IOException {
        this.engine = engine;
        this.cluster = cluster;
        this.auth = auth;
        this.http = tls.hasKeyStore() ? httpsServer(port, tls) : HttpServer.create(new InetSocketAddress(port), 0);
        this.http.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        this.http.createContext("/api/workflows", securedApi(guard(this::workflows)));
        this.http.createContext("/api/instances", securedApi(guard(this::instances)));
        this.http.createContext("/api/signals", securedApi(guard(this::signals)));
        this.http.createContext("/api/schedules", securedApi(guard(this::schedules)));
        this.http.createContext("/api/cluster", securedApi(guard(this::clusterView)));
        this.http.createContext("/api/auth", guard(this::authInfo));       // open: whether login is required
        this.http.createContext("/healthz", ex -> sendText(ex, 200, "text/plain", "ok"));  // always open
        this.http.createContext("/", securedPage(this::staticFile));
        auth.install(this.http);   // the authenticator adds its own open endpoints (login, callback, logout)
        if (!auth.required()) {
            LOG.log(System.Logger.Level.WARNING,
                    "dashboard on port " + port + " is UNAUTHENTICATED; set WIGGLE_DASHBOARD_PASSWORD to require login");
        }
        if (!tls.hasKeyStore()) {
            LOG.log(System.Logger.Level.WARNING,
                    "dashboard on port " + port + " is PLAINTEXT HTTP; set WIGGLE_TLS_KEYSTORE to serve HTTPS");
        }
    }

    /** An {@link HttpsServer} using the configured keystore, requiring client certs when a truststore is set. */
    private static HttpsServer httpsServer(int port, Tls.Options tls) throws IOException {
        HttpsServer https = HttpsServer.create(new InetSocketAddress(port), 0);
        javax.net.ssl.SSLContext ctx = Tls.sslContext(tls);
        boolean mutual = tls.hasTrustStore();
        https.setHttpsConfigurator(new HttpsConfigurator(ctx) {
            @Override public void configure(HttpsParameters params) {
                javax.net.ssl.SSLParameters p = ctx.getDefaultSSLParameters();
                if (mutual) p.setNeedClientAuth(true);   // mTLS: reject clients without a trusted cert
                params.setSSLParameters(p);
            }
        });
        LOG.log(System.Logger.Level.INFO,
                "dashboard TLS enabled" + (mutual ? " with required client certificates (mTLS)" : ""));
        return https;
    }

    public HttpDashboard start() {
        http.start();
        return this;
    }

    public int port() {
        return http.getAddress().getPort();
    }

    @Override public void close() {
        http.stop(0);
    }

    /** GET /api/workflows lists names; GET /api/workflows/{name} returns the compiled graph. */
    private void workflows(HttpExchange ex) throws IOException {
        requireGet(ex);
        String[] parts = subPath(ex, "/api/workflows");
        if (parts.length == 0) {
            sendJson(ex, 200, Map.of("workflows", engine.workflowNames()));
            return;
        }
        if (parts.length != 1) {
            sendError(ex, 404, "not found");
            return;
        }
        var def = engine.latestDefinition(parts[0]).orElse(null);
        if (def == null) sendError(ex, 404, "no such workflow");
        else sendJson(ex, 200, def.toJson());
    }

    private void clusterView(HttpExchange ex) throws IOException {
        requireGet(ex);
        long now = System.currentTimeMillis();
        long deadAfter = cluster.deadAfterMillis();
        List<Object> members = new ArrayList<>();
        for (ServerNode n : cluster.members()) {
            members.add(memberMap(n, now, deadAfter));
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("nodeId", cluster.nodeId());
        out.put("leader", cluster.isLeader());
        out.put("members", members);
        sendJson(ex, 200, out);
    }

    private static Map<String, Object> memberMap(ServerNode n, long now, long deadAfter) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", n.id);
        m.put("name", n.name);
        m.put("workers", n.workers);
        m.put("leader", n.leader);
        m.put("alive", (now - n.lastHeartbeat) < deadAfter);
        m.put("lastHeartbeat", n.lastHeartbeat);
        return m;
    }

    /** Dispatches "", "/{id}" and "/{id}/cancel" under /api/instances. */
    private void instances(HttpExchange ex) throws IOException {
        String[] parts = subPath(ex, "/api/instances");
        if (parts.length == 0) {
            listInstances(ex);
        } else if (parts.length == 2 && parts[1].equals("cancel")) {
            cancelInstance(ex, parts[0]);
        } else if (parts.length == 3 && parts[1].equals("signal")) {
            signalInstance(ex, parts[0], parts[2]);
        } else if (parts.length == 1) {
            instanceDetail(ex, parts[0]);
        } else {
            sendError(ex, 404, "not found");
        }
    }

    private void listInstances(HttpExchange ex) throws IOException {
        requireGet(ex);
        Map<String, String> q = query(ex.getRequestURI());
        String workflow = emptyToNull(q.get("workflow"));
        String status = emptyToNull(q.get("status"));
        int limit = parseInt(q.get("limit"), 100);
        List<Object> list = new ArrayList<>();
        for (InstanceView v : engine.list(workflow, status, limit)) list.add(instanceMap(v));
        sendJson(ex, 200, Map.of("instances", list));
    }

    private void cancelInstance(HttpExchange ex, String id) throws IOException {
        if (!ex.getRequestMethod().equals("POST")) {
            sendError(ex, 405, "POST required");
            return;
        }
        String reason = emptyToNull(query(ex.getRequestURI()).get("reason"));
        engine.cancel(id, reason == null ? "cancelled from dashboard" : reason);
        sendJson(ex, 200, Map.of("ok", true));
    }

    /** POST /api/instances/{id}/signal/{name}: delivers a signal; the JSON body merges into the context. */
    private void signalInstance(HttpExchange ex, String id, String name) throws IOException {
        if (!ex.getRequestMethod().equals("POST")) {
            sendError(ex, 405, "POST required");
            return;
        }
        engine.signal(id, name, readJsonBody(ex));
        sendJson(ex, 200, Map.of("ok", true));
    }

    private void instanceDetail(HttpExchange ex, String id) throws IOException {
        requireGet(ex);
        InstanceView v = engine.instance(id).orElse(null);
        if (v == null) {
            sendError(ex, 404, "no such instance");
            return;
        }
        List<Object> tokens = new ArrayList<>();
        for (Token t : engine.tokens(id)) tokens.add(tokenMap(t));
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("instance", instanceMap(v));
        out.put("tokens", tokens);
        sendJson(ex, 200, out);
    }

    /** GET /api/signals lists the waits pending an external delivery. */
    private void signals(HttpExchange ex) throws IOException {
        requireGet(ex);
        int limit = parseInt(query(ex.getRequestURI()).get("limit"), 200);
        List<Object> list = new ArrayList<>();
        for (Token t : engine.pendingSignals(limit)) list.add(signalMap(t));
        sendJson(ex, 200, Map.of("signals", list));
    }

    /** GET lists schedules; POST {workflow, everyMillis|cron, context?} creates; DELETE /{id} removes. */
    private void schedules(HttpExchange ex) throws IOException {
        String[] parts = subPath(ex, "/api/schedules");
        switch (ex.getRequestMethod()) {
            case "GET" -> listSchedules(ex);
            case "POST" -> createSchedule(ex);
            case "DELETE" -> deleteSchedule(ex, parts);
            default -> sendError(ex, 405, "GET, POST or DELETE");
        }
    }

    private void listSchedules(HttpExchange ex) throws IOException {
        List<Object> list = new ArrayList<>();
        for (Rows.Schedule sched : engine.schedules()) list.add(scheduleMap(sched));
        sendJson(ex, 200, Map.of("schedules", list));
    }

    private void createSchedule(HttpExchange ex) throws IOException {
        Map<String, Object> body = com.wiggle.core.Json.asObject(readJsonBody(ex));
        String workflow = String.valueOf(body.get("workflow"));
        String id;
        if (body.get("cron") != null) {
            id = engine.createCronSchedule(workflow, String.valueOf(body.get("cron")), body.get("context"));
        } else {
            long everyMillis = ((Number) body.get("everyMillis")).longValue();
            id = engine.createSchedule(workflow, java.time.Duration.ofMillis(everyMillis), body.get("context"));
        }
        sendJson(ex, 200, Map.of("id", id));
    }

    private void deleteSchedule(HttpExchange ex, String[] parts) throws IOException {
        if (parts.length != 1) {
            sendError(ex, 404, "not found");
            return;
        }
        engine.deleteSchedule(parts[0]);
        sendJson(ex, 200, Map.of("ok", true));
    }

    /** The path segments after {@code prefix}: [] for the collection, ["id"], or ["id", "verb"]. */
    private static String[] subPath(HttpExchange ex, String prefix) {
        String rest = ex.getRequestURI().getPath().substring(prefix.length());
        if (rest.isEmpty() || rest.equals("/")) return new String[0];
        return rest.substring(1).split("/");
    }

    private static Map<String, Object> signalMap(Token t) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("instanceId", t.instanceId);
        m.put("workflow", t.workflow);
        m.put("signal", t.activity);               // the signal's name (set when parked)
        m.put("deadline", t.availableAt);          // 0 = no deadline
        m.put("createdAt", t.createdAt);
        return m;
    }

    private static Map<String, Object> scheduleMap(Rows.Schedule s) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", s.id);
        m.put("workflow", s.workflow);
        m.put("everyMillis", s.intervalMillis);
        if (s.cron != null) m.put("cron", s.cron);
        m.put("nextFireAt", s.nextFireAt);
        m.put("createdAt", s.createdAt);
        return m;
    }

    private static Map<String, Object> instanceMap(InstanceView v) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", v.id());
        m.put("workflow", v.workflow());
        m.put("version", v.version());
        m.put("status", v.status());
        m.put("terminationReason", v.terminationReason());
        m.put("error", v.error());
        m.put("context", v.context());
        m.put("createdAt", v.createdAt());
        m.put("updatedAt", v.updatedAt());
        return m;
    }

    private static Map<String, Object> tokenMap(Token t) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", t.id);
        m.put("nodeId", t.nodeId);
        m.put("kind", t.kind == null ? null : t.kind.name());
        m.put("status", t.status == null ? null : t.status.name());
        m.put("activity", t.activity);
        m.put("queue", t.queue);
        m.put("attempt", t.attempt);
        m.put("availableAt", t.availableAt);
        m.put("leaseOwner", t.leaseOwner);
        m.put("leaseExpiresAt", t.leaseExpiresAt);
        m.put("lastError", t.lastError);
        m.put("updatedAt", t.updatedAt);
        return m;
    }

    /**
     * Serves the compiled ClojureScript single-page app from classpath resources under
     * {@code /dashboard/}. The bundle is produced by {@code make cljs}; if it has not been
     * built into the jar the dashboard responds 503. Unknown SPA routes fall through to
     * {@code index.html} for client-side routing.
     */
    private void staticFile(HttpExchange ex) throws IOException {
        String path = ex.getRequestURI().getPath();
        if (path.startsWith("/api/")) { sendError(ex, 404, "unknown endpoint"); return; }

        byte[] bundle = resource("dashboard/index.html");
        if (bundle == null) {                       // no CLJS build present
            sendError(ex, 503, "dashboard UI not built -- run `make cljs`");
            return;
        }
        String rel = path.equals("/") ? "index.html" : path.substring(1);
        if (rel.contains("..")) { sendError(ex, 400, "bad path"); return; }
        byte[] body = resource("dashboard/" + rel);
        if (body == null) { sendBytes(ex, 200, HTML, bundle); return; }   // SPA fallback route
        sendBytes(ex, 200, contentType(rel), body);
    }

    private static final String HTML = "text/html; charset=utf-8";

    /** Reads a classpath resource fully, or null if it does not exist. */
    private static byte[] resource(String name) throws IOException {
        try (var in = HttpDashboard.class.getClassLoader().getResourceAsStream(name)) {
            return in == null ? null : in.readAllBytes();
        }
    }

    private static String contentType(String path) {
        if (path.endsWith(".html")) return HTML;
        if (path.endsWith(".js")) return "text/javascript; charset=utf-8";
        if (path.endsWith(".css")) return "text/css; charset=utf-8";
        if (path.endsWith(".json") || path.endsWith(".map")) return "application/json; charset=utf-8";
        if (path.endsWith(".svg")) return "image/svg+xml";
        if (path.endsWith(".woff2")) return "font/woff2";
        return "application/octet-stream";
    }

    /** Turns a thrown exception into a clean 400/500 (see {@link DashboardHttp#guarded}). */
    private HttpHandler guard(DashboardHttp.ThrowingHandler h) {
        return DashboardHttp.guarded(h);
    }

    /** Guards a JSON API handler: an unauthenticated call gets 401 (with the auth's challenge for curl). */
    private HttpHandler securedApi(HttpHandler h) {
        return ex -> {
            if (!auth.required() || auth.authenticate(ex).isPresent()) {
                h.handle(ex);
                return;
            }
            String challenge = auth.apiChallenge();
            if (challenge != null) ex.getResponseHeaders().set("WWW-Authenticate", challenge);
            sendError(ex, 401, "authentication required");
        };
    }

    /** Guards a page handler: an unauthenticated browser is redirected to the login location. */
    private HttpHandler securedPage(HttpHandler h) {
        return ex -> {
            if (!auth.required() || auth.authenticate(ex).isPresent()) h.handle(ex);
            else redirect(ex, auth.loginLocation(ex));
        };
    }

    /** GET /api/auth: whether login is required, plus whatever the authenticator exposes to the SPA. */
    private void authInfo(HttpExchange ex) throws IOException {
        requireGet(ex);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("required", auth.required());
        out.putAll(auth.describe(ex));
        sendJson(ex, 200, out);
    }
}
