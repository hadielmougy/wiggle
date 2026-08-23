package dev.wiggle.server.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;
import dev.wiggle.core.InstanceView;
import dev.wiggle.core.Json;
import dev.wiggle.core.Tls;
import dev.wiggle.server.cluster.ClusterManager;
import dev.wiggle.server.engine.WorkflowEngine;
import dev.wiggle.server.store.Rows;
import dev.wiggle.server.store.Rows.ServerNode;
import dev.wiggle.server.store.Rows.Token;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * A small, dependency-free read-only web dashboard. It runs on the JDK's built-in
 * {@link HttpServer} and calls the in-process engine directly -- no gRPC hop, no proxy, no
 * build step. A single static page polls the JSON endpoints below.
 *
 * <p>Every node can run its own dashboard; because they share the database, any node's view
 * is the whole system's. The endpoints are read-only except for cancelling an instance.
 *
 * <p><b>Access control.</b> When a password is supplied, every endpoint -- the SPA and the whole
 * JSON API -- requires authentication against a single admin account, by either a session cookie
 * (from the {@code /login} form) or HTTP Basic credentials (for {@code curl}/programmatic use).
 * An unauthenticated browser hitting a page is redirected to {@code /login}; an unauthenticated
 * API call gets 401. {@code /healthz}, {@code /login} and {@code /api/login} are always open.
 * With no password the dashboard is unauthenticated (a warning is logged); front it with TLS or a
 * private network in that case, since credentials travel in cleartext over plain HTTP.
 */
public final class HttpDashboard implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(HttpDashboard.class.getName());
    private static final String SESSION_COOKIE = "wiggle_session";
    private static final long SESSION_TTL_MILLIS = 12 * 60 * 60 * 1000L;   // 12 hours

    private final HttpServer http;
    private final WorkflowEngine engine;
    private final ClusterManager cluster;
    private final String user;
    private final String password;
    private final boolean tlsEnabled;
    private final java.security.SecureRandom random = new java.security.SecureRandom();
    /** Live session tokens -> expiry epoch millis. Per-node (nodes don't share sessions). */
    private final Map<String, Long> sessions = new java.util.concurrent.ConcurrentHashMap<>();

    public HttpDashboard(WorkflowEngine engine, ClusterManager cluster, int port) throws IOException {
        this(engine, cluster, port, "admin", null, Tls.Options.DISABLED);
    }

    public HttpDashboard(WorkflowEngine engine, ClusterManager cluster, int port,
                         String user, String password) throws IOException {
        this(engine, cluster, port, user, password, Tls.Options.DISABLED);
    }

    public HttpDashboard(WorkflowEngine engine, ClusterManager cluster, int port,
                         String user, String password, Tls.Options tls) throws IOException {
        this.engine = engine;
        this.cluster = cluster;
        this.user = user == null || user.isBlank() ? "admin" : user;
        this.password = password == null || password.isBlank() ? null : password;
        this.tlsEnabled = tls.hasKeyStore();
        this.http = tls.hasKeyStore() ? httpsServer(port, tls) : HttpServer.create(new InetSocketAddress(port), 0);
        this.http.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        this.http.createContext("/api/workflows", securedApi(guard(this::workflows)));
        this.http.createContext("/api/instances", securedApi(guard(this::instances)));
        this.http.createContext("/api/signals", securedApi(guard(this::signals)));
        this.http.createContext("/api/schedules", securedApi(guard(this::schedules)));
        this.http.createContext("/api/cluster", securedApi(guard(this::clusterView)));
        this.http.createContext("/api/login", guard(this::login));       // open: establishes a session
        this.http.createContext("/api/auth", guard(this::auth));         // open: whether login is required
        this.http.createContext("/healthz", ex -> sendText(ex, 200, "text/plain", "ok"));  // always open
        this.http.createContext("/login", guard(this::loginPage));       // open: the login form
        this.http.createContext("/logout", guard(this::logout));         // clears the session, back to /login
        this.http.createContext("/", securedPage(this::staticFile));
        if (this.password == null) {
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
        Map<String, Object> body = dev.wiggle.core.Json.asObject(readJsonBody(ex));
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

    /** Wraps a handler so any thrown exception becomes a clean 400/500 instead of a dropped connection. */
    private HttpHandler guard(ThrowingHandler h) {
        return ex -> {
            try {
                h.handle(ex);
            } catch (IllegalArgumentException e) {
                sendError(ex, 400, e.getMessage());
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.WARNING, "dashboard request failed: " + e);
                sendError(ex, 500, e.getMessage());
            }
        };
    }

    /** Guards a JSON API handler: an unauthenticated call gets 401 (with a Basic challenge for curl). */
    private HttpHandler securedApi(HttpHandler h) {
        if (password == null) return h;          // unauthenticated mode
        return ex -> {
            if (authorized(ex)) {
                h.handle(ex);
            } else {
                ex.getResponseHeaders().set("WWW-Authenticate", "Basic realm=\"Wiggle\", charset=\"UTF-8\"");
                sendError(ex, 401, "authentication required");
            }
        };
    }

    /** Guards a page handler: an unauthenticated browser is redirected to the login form. */
    private HttpHandler securedPage(HttpHandler h) {
        if (password == null) return h;          // unauthenticated mode
        return ex -> {
            if (authorized(ex)) h.handle(ex);
            else redirect(ex, "/login");
        };
    }

    /** Authenticated by either a live session cookie (browser) or Basic credentials (curl/API). */
    private boolean authorized(HttpExchange ex) {
        return validSession(ex) || validBasic(ex.getRequestHeaders().getFirst("Authorization"));
    }

    // ---------------------------------------------------------------- sessions & login

    /** POST {user, password}: on match, mints a session cookie; otherwise 401. */
    private void login(HttpExchange ex) throws IOException {
        if (!ex.getRequestMethod().equals("POST")) { sendError(ex, 405, "POST required"); return; }
        Map<String, Object> body = Json.asObject(readJsonBody(ex));
        String u = String.valueOf(body.get("user"));
        String p = String.valueOf(body.get("password"));
        if (password != null && constantTimeEquals(u, user) && constantTimeEquals(p, password)) {
            String token = newToken();
            sessions.put(token, System.currentTimeMillis() + SESSION_TTL_MILLIS);
            ex.getResponseHeaders().add("Set-Cookie", sessionCookie(token, SESSION_TTL_MILLIS / 1000));
            sendJson(ex, 200, Map.of("ok", true));
        } else {
            sendError(ex, 401, "invalid credentials");
        }
    }

    /** GET /logout: drops the session and returns to the login form. */
    private void logout(HttpExchange ex) throws IOException {
        String token = sessionToken(ex);
        if (token != null) sessions.remove(token);
        ex.getResponseHeaders().add("Set-Cookie", sessionCookie("", 0));   // expire immediately
        redirect(ex, "/login");
    }

    /** GET /login: the form (or a bounce to the app if already in, or if auth is off). */
    private void loginPage(HttpExchange ex) throws IOException {
        if (password == null || authorized(ex)) { redirect(ex, "/"); return; }
        sendText(ex, 200, HTML, LOGIN_HTML);
    }

    /** GET /api/auth: whether login is required and, if so, the admin username -- for the SPA header. */
    private void auth(HttpExchange ex) throws IOException {
        requireGet(ex);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("required", password != null);
        out.put("user", password != null ? user : null);
        sendJson(ex, 200, out);
    }

    private boolean validSession(HttpExchange ex) {
        String token = sessionToken(ex);
        if (token == null) return false;
        Long expiry = sessions.get(token);
        if (expiry == null) return false;
        if (expiry < System.currentTimeMillis()) { sessions.remove(token); return false; }
        return true;
    }

    /** Extracts the {@code wiggle_session} cookie value, or null. */
    private static String sessionToken(HttpExchange ex) {
        String header = ex.getRequestHeaders().getFirst("Cookie");
        if (header == null) return null;
        for (String pair : header.split(";")) {
            String c = pair.trim();
            if (c.startsWith(SESSION_COOKIE + "=")) {
                String v = c.substring(SESSION_COOKIE.length() + 1);
                return v.isEmpty() ? null : v;
            }
        }
        return null;
    }

    private String newToken() {
        byte[] bytes = new byte[32];
        random.nextBytes(bytes);
        return java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    private String sessionCookie(String token, long maxAgeSeconds) {
        return SESSION_COOKIE + "=" + token + "; Max-Age=" + maxAgeSeconds
                + "; Path=/; HttpOnly; SameSite=Strict" + (tlsEnabled ? "; Secure" : "");
    }

    /** True iff {@code header} is {@code Basic <base64(user:password)>} matching the admin account. */
    private boolean validBasic(String header) {
        if (header == null || !header.regionMatches(true, 0, "Basic ", 0, 6)) return false;
        String decoded;
        try {
            decoded = new String(java.util.Base64.getDecoder().decode(header.substring(6).trim()), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException badBase64) {
            return false;
        }
        int colon = decoded.indexOf(':');
        if (colon < 0) return false;
        // Constant-time comparison of both fields so a match can't be timed out character by character.
        boolean userOk = constantTimeEquals(decoded.substring(0, colon), user);
        boolean passOk = constantTimeEquals(decoded.substring(colon + 1), password);
        return userOk & passOk;
    }

    private static boolean constantTimeEquals(String a, String b) {
        return java.security.MessageDigest.isEqual(
                a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
    }

    private static void redirect(HttpExchange ex, String location) throws IOException {
        ex.getResponseHeaders().set("Location", location);
        ex.sendResponseHeaders(302, -1);
        ex.close();
    }

    private interface ThrowingHandler { void handle(HttpExchange ex) throws IOException; }

    private static void requireGet(HttpExchange ex) {
        if (!ex.getRequestMethod().equals("GET")) throw new IllegalArgumentException("GET required");
    }

    private static Map<String, String> query(URI uri) {
        Map<String, String> out = new LinkedHashMap<>();
        String q = uri.getQuery();
        if (q == null) return out;
        for (String pair : q.split("&")) {
            int i = pair.indexOf('=');
            if (i < 0) out.put(pair, "");
            else out.put(pair.substring(0, i), java.net.URLDecoder.decode(pair.substring(i + 1), StandardCharsets.UTF_8));
        }
        return out;
    }

    private static String emptyToNull(String s) { return s == null || s.isBlank() ? null : s; }

    /** Parses the request body as JSON; an empty body is an empty object. */
    private static Object readJsonBody(HttpExchange ex) throws IOException {
        byte[] raw = ex.getRequestBody().readAllBytes();
        String body = new String(raw, StandardCharsets.UTF_8).trim();
        return body.isEmpty() ? Map.of() : Json.parse(body);
    }

    private static int parseInt(String s, int def) {
        try { return s == null ? def : Integer.parseInt(s.trim()); } catch (NumberFormatException e) { return def; }
    }

    private static void sendJson(HttpExchange ex, int status, Object body) throws IOException {
        sendBytes(ex, status, "application/json; charset=utf-8", Json.write(body).getBytes(StandardCharsets.UTF_8));
    }

    private static void sendError(HttpExchange ex, int status, String message) throws IOException {
        sendBytes(ex, status, "application/json; charset=utf-8",
                Json.write(Map.of("error", message == null ? "error" : message)).getBytes(StandardCharsets.UTF_8));
    }

    private static void sendText(HttpExchange ex, int status, String contentType, String body) throws IOException {
        sendBytes(ex, status, contentType, body.getBytes(StandardCharsets.UTF_8));
    }

    private static void sendBytes(HttpExchange ex, int status, String contentType, byte[] body) throws IOException {
        ex.getResponseHeaders().set("Content-Type", contentType);
        ex.sendResponseHeaders(status, body.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(body);
        }
    }

    /** The login form: a self-contained page (no SPA bundle) that POSTs to {@code /api/login}. */
    private static final String LOGIN_HTML = """
            <!doctype html>
            <html lang="en">
            <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <title>Wiggle — sign in</title>
            <style>
              :root { color-scheme: light dark; --bg:#0e1420; --panel:#161d2b; --line:#28324a;
                      --fg:#eef1f6; --muted:#8892a6; --accent:#f5b544; }
              * { box-sizing:border-box; }
              body { margin:0; min-height:100vh; display:grid; place-items:center;
                     background:var(--bg); color:var(--fg); font:15px/1.5 system-ui,sans-serif; }
              .card { width:min(360px,92vw); background:var(--panel); border:1px solid var(--line);
                      border-radius:14px; padding:32px 28px; }
              .brand { display:flex; align-items:center; gap:10px; font-size:20px; font-weight:800;
                       letter-spacing:.02em; margin-bottom:4px; }
              .brand .dot { color:var(--accent); }
              p.sub { margin:0 0 22px; color:var(--muted); font-size:13px; }
              label { display:block; font-size:12px; text-transform:uppercase; letter-spacing:.08em;
                      color:var(--muted); margin:14px 0 6px; }
              input { width:100%; background:#0d0f14; color:var(--fg); border:1px solid var(--line);
                      border-radius:8px; padding:10px 12px; font:inherit; }
              input:focus { outline:2px solid var(--accent); outline-offset:1px; }
              button { width:100%; margin-top:22px; background:var(--accent); color:#0e1420; border:0;
                       border-radius:8px; padding:11px; font:inherit; font-weight:700; cursor:pointer; }
              button:hover { filter:brightness(1.05); }
              .err { min-height:18px; margin-top:12px; color:#ff8080; font-size:13px; }
            </style>
            </head>
            <body>
              <form class="card" id="f">
                <div class="brand"><span class="dot">🌀</span> WIGGLE</div>
                <p class="sub">Sign in to the dashboard</p>
                <label for="u">Username</label>
                <input id="u" name="u" autocomplete="username" autofocus value="admin">
                <label for="p">Password</label>
                <input id="p" name="p" type="password" autocomplete="current-password">
                <button type="submit">Sign in</button>
                <div class="err" id="err" role="alert"></div>
              </form>
            <script>
              const f = document.getElementById('f'), err = document.getElementById('err');
              f.onsubmit = async (e) => {
                e.preventDefault();
                err.textContent = '';
                try {
                  const r = await fetch('/api/login', { method:'POST',
                    headers:{'Content-Type':'application/json'},
                    body: JSON.stringify({ user: u.value, password: p.value }) });
                  if (r.ok) { location.href = '/'; }
                  else { err.textContent = 'Invalid username or password'; p.value=''; p.focus(); }
                } catch (_) { err.textContent = 'Could not reach the server'; }
              };
            </script>
            </body>
            </html>
            """;
}
