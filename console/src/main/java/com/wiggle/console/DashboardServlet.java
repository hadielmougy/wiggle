package com.wiggle.console;

import com.wiggle.core.InstanceView;
import com.wiggle.core.Json;
import com.wiggle.server.auth.Accounts;
import com.wiggle.server.auth.Permissions;
import com.wiggle.server.engine.EngineException;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.ShardRetiredException;
import com.wiggle.server.store.StorageException;
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
import java.util.TreeSet;
import java.util.UUID;

/**
 * The console's whole HTTP surface as one servlet (mapped to {@code /}): the JSON {@code /api/*}
 * endpoints over a {@link DashboardData}, the login/logout endpoints, {@code /healthz}, and the static
 * SPA (served from the {@code dashboard/} classpath resources, with SPA-route fallback to index.html).
 * The JSON shapes match what the dashboard SPA expects (see {@link DashboardJson}).
 */
public final class DashboardServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;
    private static final System.Logger LOG = System.getLogger(DashboardServlet.class.getName());
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
            if (path.equals("/setup")) { setupPage(res); return; }
            if (path.equals("/api/setup")) { setup(req, res); return; }
            if (path.equals("/healthz")) { text(res, 200, "ok"); return; }   // k8s probe for the console pod
            if (path.equals("/api/cluster")) { clusterView(res); return; }
            if (path.equals("/api/signals")) { signals(req, res); return; }
            if (path.equals("/api/backlog")) { backlog(req, res); return; }
            if (path.equals("/api/stats")) { stats(req, res); return; }
            if (path.equals("/api/password")) { changeOwnPassword(req, res); return; }
            if (path.startsWith("/api/users")) { users(req, res, sub(path, "/api/users")); return; }
            if (path.startsWith("/api/roles")) { roles(req, res, sub(path, "/api/roles")); return; }
            if (path.equals("/api/audit")) { audit(req, res); return; }
            if (path.equals("/api/search")) { search(req, res); return; }
            if (path.startsWith("/api/credentials")) { credentials(req, res, sub(path, "/api/credentials")); return; }
            if (path.startsWith("/api/workflows")) { workflows(res, sub(path, "/api/workflows")); return; }
            if (path.startsWith("/api/instances")) { instances(req, res, sub(path, "/api/instances")); return; }
            if (path.startsWith("/api/schedules")) { schedules(req, res, sub(path, "/api/schedules")); return; }
            if (path.startsWith("/api/")) { error(res, 404, "unknown endpoint"); return; }
            staticFile(res, path);
        } catch (IllegalArgumentException | IllegalStateException e) {
            // A rejected user name, password or role: the caller's mistake, not the console's.
            error(res, 400, e.getMessage());
        } catch (EngineException e) {
            error(res, e.statusCode(), e.getMessage());
        } catch (ShardRetiredException e) {
            error(res, 404, e.getMessage());
        } catch (StorageException e) {
            if (e.repeatable()) {
                error(res, 503, "storage temporarily unavailable (ref " + logged(e) + ")");
            } else {
                error(res, 500, "internal error (ref " + logged(e) + ")");
            }
        } catch (RuntimeException e) {
            error(res, 500, "internal error (ref " + logged(e) + ")");
        }
    }


    /** Logs {@code e} with its detail and returns the reference the response carries in its place. */
    private static String logged(RuntimeException e) {
        String ref = UUID.randomUUID().toString().substring(0, 8);
        LOG.log(System.Logger.Level.WARNING, "portal request failed [" + ref + "]", e);
        return ref;
    }

    private void authInfo(HttpServletRequest req, HttpServletResponse res) throws IOException {
        ConsoleAuth.Principal p = auth.principal(req);   // null if auth is required and the caller isn't authenticated
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("required", auth.required());
        out.put("setupRequired", auth.setupRequired());
        out.put("user", p == null ? null : p.user());
        out.put("permissions", p == null ? List.of() : List.copyOf(new TreeSet<>(p.permissions())));
        out.put("role", p == null ? null : p.writes() ? "admin" : "viewer");
        out.put("canWrite", p != null && p.writes());
        // A built-in account's password lives in the environment, so the portal cannot change it.
        out.put("canChangePassword", auth.accounts() != null && p != null && p.user() != null && !p.builtin());
        out.put("managesUsers", auth.accounts() != null);
        out.put("searchEnabled", data.searchEnabled());
        out.put("semanticEnabled", data.semanticEnabled());
        json(res, 200, out);
    }

    /** The principal the filter resolved for this request. */
    private ConsoleAuth.Principal principal(HttpServletRequest req) {
        ConsoleAuth.Principal p = auth.principal(req);
        if (p == null) throw new IllegalStateException("not signed in");
        return p;
    }

    /** The signed-in account's name, for the audit; null in open mode. */
    private String actor(HttpServletRequest req) {
        return principal(req).user();
    }

    /** Whether the caller may do {@code action} on {@code scope}; answers 403 when not. */
    private boolean permitted(HttpServletRequest req, HttpServletResponse res, String action, String scope)
            throws IOException {
        if (principal(req).allows(action, scope)) return true;
        error(res, 403, "permission '" + action + "' on '" + scope + "' required");
        return false;
    }

    /** The accounts managed on the auth shard. The filter has checked {@code user.manage}. */
    private void users(HttpServletRequest req, HttpServletResponse res, String[] parts) throws IOException {
        Accounts accounts = auth.accounts();
        if (accounts == null) { error(res, 404, "this portal manages no users"); return; }
        boolean fallback = auth.hasBuiltinAdmin();
        switch (req.getMethod()) {
            case "GET" -> {
                if (parts.length != 0) { error(res, 404, "not found"); return; }
                List<Object> list = new ArrayList<>();
                for (String name : auth.builtinNames()) {
                    String role = name.equals(auth.user()) ? Permissions.ADMIN : Permissions.VIEWER;
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("name", name);
                    m.put("role", role);
                    m.put("roles", List.of(role));
                    m.put("builtin", true);
                    m.put("disabled", false);
                    list.add(m);
                }
                for (Accounts.User u : accounts.users()) {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("name", u.name());
                    m.put("role", u.roles().isEmpty() ? null : u.roles().getFirst());
                    m.put("roles", u.roles());
                    m.put("builtin", false);
                    m.put("disabled", u.disabled());
                    m.put("createdAt", u.createdAt());
                    m.put("updatedAt", u.updatedAt());
                    list.add(m);
                }
                json(res, 200, Map.of("users", list));
            }
            case "POST" -> {
                Map<String, Object> body = Json.asObject(readBody(req));
                if (parts.length == 2) {
                    String name = parts[0];
                    if (auth.builtinNames().contains(name)) {
                        error(res, 400, "'" + name + "' is a built-in account set in the environment; "
                                + "change it where the server is deployed");
                        return;
                    }
                    switch (parts[1]) {
                        case "password" -> accounts.setPassword(actor(req), name, String.valueOf(body.get("password")), null);
                        case "roles" -> accounts.setRoles(actor(req), name, roleList(body), fallback);
                        case "disabled" -> accounts.setDisabled(actor(req), name, Boolean.TRUE.equals(body.get("disabled")),
                                fallback);
                        default -> { error(res, 404, "not found"); return; }
                    }
                    json(res, 200, Map.of("ok", true));
                    return;
                }
                if (parts.length != 0) { error(res, 404, "not found"); return; }
                String name = String.valueOf(body.get("user"));
                List<String> roles = roleList(body);
                accounts.create(actor(req), name, String.valueOf(body.get("password")), roles, auth.builtinNames(),
                        fallback);
                json(res, 200, Map.of("user", name, "roles", roles));
            }
            case "DELETE" -> {
                if (parts.length != 1) { error(res, 404, "not found"); return; }
                String name = parts[0];
                if (auth.builtinNames().contains(name)) {
                    error(res, 400, "'" + name + "' is a built-in account set in the environment; "
                            + "remove it where the server is deployed");
                    return;
                }
                accounts.delete(actor(req), name, fallback);
                json(res, 200, Map.of("ok", true));
            }
            default -> error(res, 405, "GET, POST or DELETE");
        }
    }

    /** {@code roles} as a list, or the one {@code role}, lower-cased; {@code operator} is the old name for admin. */
    private static List<String> roleList(Map<String, Object> body) {
        List<String> out = new ArrayList<>();
        Object roles = body.get("roles");
        if (roles instanceof List<?> l) l.forEach(r -> out.add(roleName(r)));
        else if (body.get("role") != null) out.add(roleName(body.get("role")));
        return out;
    }

    private static String roleName(Object raw) {
        String r = String.valueOf(raw).trim().toLowerCase();
        return r.equals("operator") ? Permissions.ADMIN : r;
    }

    /** Roles: named permission sets. The filter has checked {@code user.manage}. */
    private void roles(HttpServletRequest req, HttpServletResponse res, String[] parts) throws IOException {
        Accounts accounts = auth.accounts();
        if (accounts == null) { error(res, 404, "this portal manages no roles"); return; }
        switch (req.getMethod()) {
            case "GET" -> {
                if (parts.length != 0) { error(res, 404, "not found"); return; }
                List<Object> list = new ArrayList<>();
                for (Rows.AuthRole r : accounts.roles()) {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("name", r.name());
                    m.put("permissions", List.copyOf(new TreeSet<>(r.permissions())));
                    m.put("builtin", r.builtin());
                    m.put("updatedAt", r.updatedAt());
                    list.add(m);
                }
                json(res, 200, Map.of("roles", list, "actions", Permissions.ACTIONS));
            }
            case "POST" -> {
                if (parts.length != 0) { error(res, 404, "not found"); return; }
                Map<String, Object> body = Json.asObject(readBody(req));
                List<String> perms = new ArrayList<>();
                Object raw = body.get("permissions");
                if (raw instanceof List<?> l) l.forEach(x -> perms.add(String.valueOf(x)));
                else if (raw != null) perms.addAll(List.of(String.valueOf(raw).trim().split("[\\s,]+")));
                String name = String.valueOf(body.get("name")).trim().toLowerCase();
                accounts.putRole(actor(req), name, perms, auth.hasBuiltinAdmin());
                json(res, 200, Map.of("ok", true));
            }
            case "DELETE" -> {
                if (parts.length != 1) { error(res, 404, "not found"); return; }
                accounts.deleteRole(actor(req), parts[0], auth.hasBuiltinAdmin());
                json(res, 200, Map.of("ok", true));
            }
            default -> error(res, 405, "GET, POST or DELETE");
        }
    }

    /**
     * Machine credentials for the gRPC API: API keys and client certificate subjects, each holding a
     * role. A new API key is in the response that creates it and nowhere else. The filter has checked
     * {@code user.manage}.
     */
    private void credentials(HttpServletRequest req, HttpServletResponse res, String[] parts) throws IOException {
        Accounts accounts = auth.accounts();
        if (accounts == null) { error(res, 404, "this portal manages no credentials"); return; }
        switch (req.getMethod()) {
            case "GET" -> {
                if (parts.length != 0) { error(res, 404, "not found"); return; }
                List<Object> list = new ArrayList<>();
                for (Rows.AuthCredential c : accounts.credentials()) {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("id", c.id());
                    m.put("kind", c.kind().equals(Rows.AuthCredential.API_KEY) ? "api-key" : "mtls");
                    m.put("subject", c.subject());
                    m.put("role", c.role());
                    m.put("createdAt", c.createdAt());
                    m.put("expiresAt", c.expiresAt());
                    list.add(m);
                }
                json(res, 200, Map.of("credentials", list));
            }
            case "POST" -> {
                if (parts.length != 0) { error(res, 404, "not found"); return; }
                Map<String, Object> body = Json.asObject(readBody(req));
                String id = String.valueOf(body.get("id")).trim();
                String role = roleName(body.get("role"));
                Long expiresAt = body.get("expiresAt") instanceof Number n && n.longValue() > 0 ? n.longValue() : null;
                String kind = String.valueOf(body.getOrDefault("kind", "api-key")).trim().toLowerCase();
                switch (kind) {
                    case "api-key" -> {
                        String key = accounts.createApiKey(actor(req), id, role, expiresAt);
                        json(res, 200, Map.of("id", id, "key", key));
                    }
                    case "mtls" -> {
                        accounts.createCertificate(actor(req), id, String.valueOf(body.get("subject")), role, expiresAt);
                        json(res, 200, Map.of("id", id));
                    }
                    default -> error(res, 400, "a credential kind is api-key or mtls, not '" + kind + "'");
                }
            }
            case "DELETE" -> {
                if (parts.length != 1) { error(res, 404, "not found"); return; }
                accounts.deleteCredential(actor(req), parts[0]);
                json(res, 200, Map.of("ok", true));
            }
            default -> error(res, 405, "GET, POST or DELETE");
        }
    }

    /** Changes to accounts, roles and sessions, oldest first after {@code after}. */
    private void audit(HttpServletRequest req, HttpServletResponse res) throws IOException {
        Accounts accounts = auth.accounts();
        if (accounts == null) { error(res, 404, "this portal manages no users"); return; }
        long after = parseLong(req.getParameter("after"), 0);
        int limit = Math.min(parseInt(req.getParameter("limit"), 100), 1000);
        List<Object> list = new ArrayList<>();
        for (Rows.AuthAudit a : accounts.auditAfter(after, limit)) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("seq", a.seq());
            m.put("at", a.at());
            m.put("actor", a.actor());
            m.put("action", a.action());
            m.put("target", a.target());
            if (!a.action().startsWith("session.")) m.put("detail", a.detail());
            list.add(m);
        }
        json(res, 200, Map.of("entries", list));
    }

    /** Self-service: the signed-in account changes its own password, proving the current one. */
    private void changeOwnPassword(HttpServletRequest req, HttpServletResponse res) throws IOException {
        if (!req.getMethod().equals("POST")) { error(res, 405, "POST required"); return; }
        Map<String, Object> body = Json.asObject(readBody(req));
        auth.changeOwnPassword(req, String.valueOf(body.get("current")), String.valueOf(body.get("password")));
        json(res, 200, Map.of("ok", true));
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

    private void setupPage(HttpServletResponse res) throws IOException {
        if (!auth.setupRequired()) { redirect(res, "/login"); return; }
        html(res, ConsoleAuth.SETUP_HTML.replace("__USER__", escape(auth.user())));
    }

    /** First run: sets the admin's password and signs it in. 409 once the portal is set up. */
    private void setup(HttpServletRequest req, HttpServletResponse res) throws IOException {
        if (!req.getMethod().equals("POST")) { error(res, 405, "POST required"); return; }
        if (!auth.setupRequired()) { error(res, 409, "the portal is already set up; sign in instead"); return; }
        Map<String, Object> body = Json.asObject(readBody(req));
        String cookie = auth.setUpAdmin(String.valueOf(body.get("password")));
        res.addHeader("Set-Cookie", cookie);
        json(res, 200, Map.of("ok", true, "user", auth.user()));
    }

    private static String escape(String s) {
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;");
    }

    private void loginPage(HttpServletRequest req, HttpServletResponse res) throws IOException {
        if (auth.setupRequired()) { redirect(res, "/setup"); return; }
        if (!auth.required() || auth.authenticated(req)) { redirect(res, "/"); return; }
        html(res, ConsoleAuth.LOGIN_HTML);
    }


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
            // Exact instance-id lookup: one row (or none).
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
        if (!permitted(req, res, Permissions.INSTANCE_CANCEL, workflowOf(id))) return;
        String reason = trimToNull(req.getParameter("reason"));
        data.cancel(id, reason == null ? "cancelled from console" : reason);
        json(res, 200, Map.of("ok", true));
    }

    private void signal(HttpServletRequest req, HttpServletResponse res, String id, String name) throws IOException {
        if (!req.getMethod().equals("POST")) { error(res, 405, "POST required"); return; }
        if (!permitted(req, res, Permissions.INSTANCE_SIGNAL, workflowOf(id))) return;
        data.signal(id, name, readBody(req));
        json(res, 200, Map.of("ok", true));
    }

    /** Full-text search, limited to the workflows the caller may read. */
    private void search(HttpServletRequest req, HttpServletResponse res) throws IOException {
        DashboardData.SearchView found = data.search(trimToNull(req.getParameter("q")),
                trimToNull(req.getParameter("workflow")), trimToNull(req.getParameter("status")),
                parseInt(req.getParameter("limit"), 50), "true".equals(req.getParameter("partial")),
                "semantic".equals(req.getParameter("mode")), Permissions.readable(principal(req).permissions())).orElse(null);
        if (found == null) { error(res, 404, "search is not enabled on this deployment"); return; }
        List<Object> hits = new ArrayList<>();
        for (DashboardData.SearchHitView h : found.hits()) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", h.instanceId());
            m.put("workflow", h.workflow());
            m.put("version", h.version());
            m.put("status", h.status());
            m.put("correlationId", h.correlationId());
            m.put("updatedAt", h.updatedAt());
            m.put("score", h.score());
            m.put("purged", h.purged());
            hits.add(m);
        }
        json(res, 200, Map.of("hits", hits, "partial", found.partial()));
    }

    /** The workflow of instance {@code id}, which scopes what may be done to it. */
    private String workflowOf(String id) {
        return data.instance(id).map(d -> d.instance().workflow())
                .orElseThrow(() -> EngineException.notFound("instance " + id));
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

    /** Dispatchable work and whether anything is polling for it. See DashboardData#backlogCoverage. */
    private void backlog(HttpServletRequest req, HttpServletResponse res) throws IOException {
        int limit = parseInt(req.getParameter("limit"), 100);
        List<DashboardData.BacklogView> slices = data.backlogCoverage(limit);
        List<Object> list = new ArrayList<>();
        int uncovered = 0, stranded = 0;
        for (DashboardData.BacklogView b : slices) {
            list.add(DashboardJson.backlog(b));
            if (!b.covered()) { uncovered++; stranded += b.readyCount(); }
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("slices", list);
        out.put("uncoveredSlices", uncovered);
        out.put("strandedTasks", stranded);
        out.put("livePollers", slices.isEmpty() ? 0 : slices.get(0).livePollers());
        json(res, 200, out);
    }

    /** Per-node durations of one workflow; see DashboardData#stepStats. */
    private void stats(HttpServletRequest req, HttpServletResponse res) throws IOException {
        String workflow = trimToNull(req.getParameter("workflow"));
        if (workflow == null) { error(res, 400, "workflow is required"); return; }
        int version = parseInt(req.getParameter("version"), 0);
        long since = parseLong(req.getParameter("since"), 0);
        int sample = parseInt(req.getParameter("sample"), 10_000);
        List<Object> nodes = new ArrayList<>();
        for (com.wiggle.core.NodeStats n : data.stepStats(workflow, version == 0 ? null : version, since, sample)) {
            nodes.add(n.toJson());
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("workflow", workflow);
        out.put("version", version);
        out.put("since", since);
        out.put("nodes", nodes);
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
                if (!permitted(req, res, Permissions.SCHEDULE_WRITE, workflow)) return;
                String id = body.get("cron") != null
                        ? data.createCronSchedule(workflow, String.valueOf(body.get("cron")), body.get("context"))
                        : data.createSchedule(workflow, Duration.ofMillis(((Number) body.get("everyMillis")).longValue()),
                                body.get("context"));
                json(res, 200, Map.of("id", id));
            }
            case "DELETE" -> {
                if (parts.length != 1) { error(res, 404, "not found"); return; }
                String workflow = data.schedules().stream().filter(x -> x.id().equals(parts[0]))
                        .map(DashboardData.ScheduleView::workflow).findFirst().orElse(null);
                if (workflow == null) { error(res, 404, "no such schedule"); return; }
                if (!permitted(req, res, Permissions.SCHEDULE_WRITE, workflow)) return;
                data.deleteSchedule(parts[0]);
                json(res, 200, Map.of("ok", true));
            }
            default -> error(res, 405, "GET, POST or DELETE");
        }
    }


    private void staticFile(HttpServletResponse res, String path) throws IOException {
        byte[] index = resource("dashboard/index.html");
        if (index == null) { error(res, 503, "dashboard UI not built -- run `make cljs`"); return; }
        String rel = path.equals("/") ? "index.html" : path.substring(1);
        if (rel.contains("..")) { error(res, 400, "bad path"); return; }
        byte[] body = resource("dashboard/" + rel);
        if (body == null) { bytes(res, 200, HTML, index); return; }   // SPA client-side route
        bytes(res, 200, contentType(rel), body);
    }


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

    private static long parseLong(String s, long def) {
        try { return s == null || s.isBlank() ? def : Long.parseLong(s.trim()); }
        catch (NumberFormatException e) { return def; }
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
