package dev.wiggle.server.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import dev.wiggle.core.InstanceView;
import dev.wiggle.core.Json;
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
 */
public final class HttpDashboard implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(HttpDashboard.class.getName());

    private final HttpServer http;
    private final WorkflowEngine engine;
    private final ClusterManager cluster;

    public HttpDashboard(WorkflowEngine engine, ClusterManager cluster, int port) throws IOException {
        this.engine = engine;
        this.cluster = cluster;
        this.http = HttpServer.create(new InetSocketAddress(port), 0);
        this.http.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        this.http.createContext("/api/workflows", guard(this::workflows));
        this.http.createContext("/api/instances", guard(this::instances));
        this.http.createContext("/api/signals", guard(this::signals));
        this.http.createContext("/api/schedules", guard(this::schedules));
        this.http.createContext("/api/cluster", guard(this::clusterView));
        this.http.createContext("/healthz", ex -> sendText(ex, 200, "text/plain", "ok"));
        this.http.createContext("/", this::staticFile);
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
    }}
