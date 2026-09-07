package com.wiggle.console;

import com.wiggle.client.WiggleClient.WiggleApiException;
import com.wiggle.core.InstanceView;
import com.wiggle.core.Json;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The console's whole HTTP surface as one servlet (mapped to {@code /}): the JSON {@code /api/*}
 * endpoints over a {@link DashboardData}, the login/logout endpoints, {@code /healthz}, and the static
 * SPA (served from the {@code dashboard/} classpath resources, with SPA-route fallback to index.html).
 * The JSON shapes match what the dashboard SPA expects (see {@link DashboardJson}).
 */
public final class DashboardServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;
    private static final String HTML = "text/html; charset=utf-8";
    private static final String JSON = "application/json; charset=utf-8";

    private final DashboardData data;
    private final ConsoleAuth auth;

    public DashboardServlet(DashboardData data, ConsoleAuth auth) {
        this.data = data;
        this.auth = auth;
    }

    @Override
    protected void service(HttpServletRequest req, HttpServletResponse res) throws IOException {
        String path = req.getRequestURI();
        try {
            if (path.equals("/api/auth")) { authInfo(req, res); return; }
            if (path.equals("/api/login")) { login(req, res); return; }
            if (path.equals("/logout")) { logout(req, res); return; }
            if (path.equals("/login")) { loginPage(req, res); return; }
            if (path.equals("/healthz")) { text(res, 200, "ok"); return; }   // k8s probe for the console pod
            if (path.equals("/api/cluster")) { clusterView(res); return; }
            if (path.equals("/api/signals")) { signals(req, res); return; }
            if (path.startsWith("/api/workflows")) { workflows(res, sub(path, "/api/workflows")); return; }
            if (path.startsWith("/api/instances")) { instances(req, res, sub(path, "/api/instances")); return; }
            if (path.startsWith("/api/schedules")) { schedules(req, res, sub(path, "/api/schedules")); return; }
            if (path.startsWith("/api/")) { error(res, 404, "unknown endpoint"); return; }
            staticFile(res, path);
        } catch (WiggleApiException e) {
            error(res, e.status(), e.getMessage());
        } catch (RuntimeException e) {
            error(res, 500, e.getMessage());
        }
    }

    // ---- auth endpoints (open; the filter lets these through) ----

    private void authInfo(HttpServletRequest req, HttpServletResponse res) throws IOException {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("required", auth.required());
        out.put("user", auth.required() ? auth.user() : null);
        json(res, 200, out);
    }

    private void login(HttpServletRequest req, HttpServletResponse res) throws IOException {
        if (!req.getMethod().equals("POST")) { error(res, 405, "POST required"); return; }
        Map<String, Object> body = Json.asObject(readBody(req));
        String cookie = auth.login(String.valueOf(body.get("user")), String.valueOf(body.get("password")));
        if (cookie != null) {
            res.addHeader("Set-Cookie", cookie);
            json(res, 200, Map.of("ok", true));
        } else {
            error(res, 401, "invalid credentials");
        }
    }

    private void logout(HttpServletRequest req, HttpServletResponse res) throws IOException {
        auth.logout(req);
        res.addHeader("Set-Cookie", auth.expiredCookie());
        res.setStatus(302);
        res.setHeader("Location", "/login");
    }

    private void loginPage(HttpServletRequest req, HttpServletResponse res) throws IOException {
        if (!auth.required() || auth.authenticated(req)) { redirect(res, "/"); return; }
        html(res, ConsoleAuth.LOGIN_HTML);
    }

    // ---- JSON API ----

    private void workflows(HttpServletResponse res, String[] parts) throws IOException {
        if (parts.length == 0) { json(res, 200, Map.of("workflows", data.workflowNames())); return; }
        if (parts.length != 1) { error(res, 404, "not found"); return; }
        Object graph = data.workflowGraph(parts[0]).orElse(null);
        if (graph == null) error(res, 404, "no such workflow");
        else json(res, 200, graph);
    }

    private void clusterView(HttpServletResponse res) throws IOException {
        json(res, 200, DashboardJson.cluster(data.cluster()));
    }

    private void instances(HttpServletRequest req, HttpServletResponse res, String[] parts) throws IOException {
        if (parts.length == 0) { listInstances(req, res); return; }
        if (parts.length == 2 && parts[1].equals("cancel")) { cancel(req, res, parts[0]); return; }
        if (parts.length == 3 && parts[1].equals("signal")) { signal(req, res, parts[0], parts[2]); return; }
        if (parts.length == 1) { instanceDetail(res, parts[0]); return; }
        error(res, 404, "not found");
    }

    private void listInstances(HttpServletRequest req, HttpServletResponse res) throws IOException {
        int limit = parseInt(req.getParameter("limit"), 100);
        String id = trimToNull(req.getParameter("id"));
        String correlation = trimToNull(req.getParameter("correlation"));
        List<InstanceView> found;
        if (id != null) {
            // Exact instance-id lookup: one row (or none), routed to the owning cell.
            found = data.instance(id).map(d -> List.of(d.instance())).orElse(List.of());
        } else if (correlation != null) {
            found = data.findByCorrelation(correlation, limit);
        } else {
            found = data.listInstances(trimToNull(req.getParameter("workflow")),
                    trimToNull(req.getParameter("status")), limit);
        }
        List<Object> list = new ArrayList<>();
        for (InstanceView v : found) list.add(DashboardJson.instance(v));
        json(res, 200, Map.of("instances", list));
    }

    private void cancel(HttpServletRequest req, HttpServletResponse res, String id) throws IOException {
        if (!req.getMethod().equals("POST")) { error(res, 405, "POST required"); return; }
        String reason = trimToNull(req.getParameter("reason"));
        data.cancel(id, reason == null ? "cancelled from console" : reason);
        json(res, 200, Map.of("ok", true));
    }

    private void signal(HttpServletRequest req, HttpServletResponse res, String id, String name) throws IOException {
        if (!req.getMethod().equals("POST")) { error(res, 405, "POST required"); return; }
        data.signal(id, name, readBody(req));
        json(res, 200, Map.of("ok", true));
    }

    private void instanceDetail(HttpServletResponse res, String id) throws IOException {
        DashboardData.InstanceDetail detail = data.instance(id).orElse(null);
        if (detail == null) { error(res, 404, "no such instance"); return; }
        List<Object> tokens = new ArrayList<>();
        for (DashboardData.TokenView t : detail.tokens()) tokens.add(DashboardJson.token(t));
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("instance", DashboardJson.instance(detail.instance()));
        out.put("tokens", tokens);
        json(res, 200, out);
    }

    private void signals(HttpServletRequest req, HttpServletResponse res) throws IOException {
        int limit = parseInt(req.getParameter("limit"), 200);
        List<Object> list = new ArrayList<>();
        for (DashboardData.SignalView t : data.pendingSignals(limit)) list.add(DashboardJson.signal(t));
        json(res, 200, Map.of("signals", list));
    }

    private void schedules(HttpServletRequest req, HttpServletResponse res, String[] parts) throws IOException {
        switch (req.getMethod()) {
            case "GET" -> {
                List<Object> list = new ArrayList<>();
                for (DashboardData.ScheduleView s : data.schedules()) list.add(DashboardJson.schedule(s));
                json(res, 200, Map.of("schedules", list));
            }
            case "POST" -> {
                Map<String, Object> body = Json.asObject(readBody(req));
                String workflow = String.valueOf(body.get("workflow"));
                String id = body.get("cron") != null
                        ? data.createCronSchedule(workflow, String.valueOf(body.get("cron")), body.get("context"))
                        : data.createSchedule(workflow, Duration.ofMillis(((Number) body.get("everyMillis")).longValue()),
                                body.get("context"));
                json(res, 200, Map.of("id", id));
            }
            case "DELETE" -> {
                if (parts.length != 1) { error(res, 404, "not found"); return; }
                data.deleteSchedule(parts[0]);
                json(res, 200, Map.of("ok", true));
            }
            default -> error(res, 405, "GET, POST or DELETE");
        }
    }

    // ---- static SPA ----

    private void staticFile(HttpServletResponse res, String path) throws IOException {
        byte[] index = resource("dashboard/index.html");
        if (index == null) { error(res, 503, "dashboard UI not built -- run `make cljs`"); return; }
        String rel = path.equals("/") ? "index.html" : path.substring(1);
        if (rel.contains("..")) { error(res, 400, "bad path"); return; }
        byte[] body = resource("dashboard/" + rel);
        if (body == null) { bytes(res, 200, HTML, index); return; }   // SPA client-side route
        bytes(res, 200, contentType(rel), body);
    }

    // ---- helpers ----

    private static String[] sub(String path, String prefix) {
        String rest = path.substring(prefix.length());
        if (rest.isEmpty() || rest.equals("/")) return new String[0];
        return rest.substring(1).split("/");
    }

    private static void json(HttpServletResponse res, int status, Object body) throws IOException {
        bytes(res, status, JSON, Json.write(body).getBytes(StandardCharsets.UTF_8));
    }

    private static void error(HttpServletResponse res, int status, String message) throws IOException {
        bytes(res, status, JSON, Json.write(Map.of("error", message == null ? "error" : message))
                .getBytes(StandardCharsets.UTF_8));
    }

    private static void text(HttpServletResponse res, int status, String body) throws IOException {
        bytes(res, status, "text/plain; charset=utf-8", body.getBytes(StandardCharsets.UTF_8));
    }

    private static void html(HttpServletResponse res, String body) throws IOException {
        bytes(res, 200, HTML, body.getBytes(StandardCharsets.UTF_8));
    }

    private static void redirect(HttpServletResponse res, String location) {
        res.setStatus(302);
        res.setHeader("Location", location);
    }

    private static void bytes(HttpServletResponse res, int status, String contentType, byte[] body) throws IOException {
        res.setStatus(status);
        res.setContentType(contentType);
        res.setContentLength(body.length);
        res.getOutputStream().write(body);
    }

    private static Object readBody(HttpServletRequest req) throws IOException {
        byte[] raw = req.getInputStream().readAllBytes();
        if (raw.length == 0) return Map.of();
        return Json.parse(new String(raw, StandardCharsets.UTF_8));
    }

    private static String trimToNull(String s) {
        return s == null || s.isBlank() ? null : s;
    }

    private static int parseInt(String s, int def) {
        if (s == null || s.isBlank()) return def;
        try { return Integer.parseInt(s.trim()); } catch (NumberFormatException e) { return def; }
    }

    private static byte[] resource(String name) throws IOException {
        try (var in = DashboardServlet.class.getClassLoader().getResourceAsStream(name)) {
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
}
