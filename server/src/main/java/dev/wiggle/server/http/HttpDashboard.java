package dev.wiggle.server.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import dev.wiggle.core.InstanceView;
import dev.wiggle.core.Json;
import dev.wiggle.server.cluster.ClusterManager;
import dev.wiggle.server.engine.WorkflowEngine;
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
        this.http.createContext("/api/tasks", guard(this::tasks));
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

    private void workflows(HttpExchange ex) throws IOException {
        requireGet(ex);
        sendJson(ex, 200, Map.of("workflows", engine.definitions().names()));
    }

    private void clusterView(HttpExchange ex) throws IOException {
        requireGet(ex);
        long now = System.currentTimeMillis();
        long deadAfter = cluster.deadAfterMillis();
        List<Object> members = new ArrayList<>();
        for (ServerNode n : cluster.members()) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", n.id);
            m.put("name", n.name);
            m.put("workers", n.workers);
            m.put("leader", n.leader);
            m.put("alive", (now - n.lastHeartbeat) < deadAfter);
            m.put("lastHeartbeat", n.lastHeartbeat);
            members.add(m);
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("nodeId", cluster.nodeId());
        out.put("leader", cluster.isLeader());
        out.put("members", members);
        sendJson(ex, 200, out);
    }

    private void instances(HttpExchange ex) throws IOException {
        String path = ex.getRequestURI().getPath();
        String rest = path.substring("/api/instances".length());   // "", "/{id}", "/{id}/cancel"

        if (rest.isEmpty() || rest.equals("/")) {
            requireGet(ex);
            Map<String, String> q = query(ex.getRequestURI());
            String workflow = emptyToNull(q.get("workflow"));
            String status = emptyToNull(q.get("status"));
            int limit = parseInt(q.get("limit"), 100);
            List<Object> list = new ArrayList<>();
            for (InstanceView v : engine.list(workflow, status, limit)) list.add(instanceMap(v));
            sendJson(ex, 200, Map.of("instances", list));
            return;
        }

        String[] parts = rest.substring(1).split("/");   // drop leading '/'
        String id = parts[0];
        if (parts.length == 2 && parts[1].equals("cancel")) {
            if (!ex.getRequestMethod().equals("POST")) { sendError(ex, 405, "POST required"); return; }
            String reason = emptyToNull(query(ex.getRequestURI()).get("reason"));
            engine.cancel(id, reason == null ? "cancelled from dashboard" : reason);
            sendJson(ex, 200, Map.of("ok", true));
            return;
        }
        if (parts.length != 1) { sendError(ex, 404, "not found"); return; }

        requireGet(ex);
        InstanceView v = engine.instance(id).orElse(null);
        if (v == null) { sendError(ex, 404, "no such instance"); return; }
        List<Object> tokens = new ArrayList<>();
        for (Token t : engine.tokens(id)) tokens.add(tokenMap(t));
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("instance", instanceMap(v));
        out.put("tokens", tokens);
        sendJson(ex, 200, out);
    }

    private void tasks(HttpExchange ex) throws IOException {
        String rest = ex.getRequestURI().getPath().substring("/api/tasks".length());   // "", "/{id}/complete"

        if (rest.isEmpty() || rest.equals("/")) {
            requireGet(ex);
            int limit = parseInt(query(ex.getRequestURI()).get("limit"), 200);
            List<Object> list = new ArrayList<>();
            for (Token t : engine.pendingUserTasks(limit)) list.add(taskMap(t));
            sendJson(ex, 200, Map.of("tasks", list));
            return;
        }

        String[] parts = rest.substring(1).split("/");
        if (parts.length == 2 && parts[1].equals("complete")) {
            if (!ex.getRequestMethod().equals("POST")) { sendError(ex, 405, "POST required"); return; }
            Object result = readJsonBody(ex);
            engine.completeUserTask(parts[0], result);
            sendJson(ex, 200, Map.of("ok", true));
            return;
        }
        sendError(ex, 404, "not found");
    }

    private static Map<String, Object> taskMap(Token t) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", t.id);
        m.put("instanceId", t.instanceId);
        m.put("workflow", t.workflow);
        m.put("name", t.activity);                 // the user task's human name (set when parked)
        m.put("deadline", t.availableAt);          // 0 = no deadline
        m.put("createdAt", t.createdAt);
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

    private void staticFile(HttpExchange ex) throws IOException {
        String path = ex.getRequestURI().getPath();
        if (path.startsWith("/api/")) { sendError(ex, 404, "unknown endpoint"); return; }
        if (!path.equals("/") && !path.equals("/index.html")) { sendError(ex, 404, "not found"); return; }
        sendText(ex, 200, "text/html; charset=utf-8", INDEX_HTML);
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
    }

    // The whole UI: one self-contained page that polls the JSON endpoints above.
    private static final String INDEX_HTML = """
            <!doctype html>
            <html lang="en">
            <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <title>Wiggle</title>
            <style>
              :root { color-scheme: light dark; --bg:#0f1115; --panel:#171a21; --line:#2a2f3a;
                      --fg:#e6e8ec; --muted:#8b93a1; --accent:#5b9dff; }
              * { box-sizing: border-box; }
              body { margin:0; font:14px/1.5 system-ui,sans-serif; background:var(--bg); color:var(--fg); }
              header { display:flex; align-items:center; gap:16px; padding:12px 20px; border-bottom:1px solid var(--line); }
              header h1 { font-size:16px; margin:0; letter-spacing:.5px; }
              header .cluster { color:var(--muted); font-size:12px; margin-left:auto; }
              .dot { display:inline-block; width:8px; height:8px; border-radius:50%; background:#3ecf7a; margin-right:4px; }
              .dot.leader { background:var(--accent); }
              main { display:grid; grid-template-columns: 1fr 1fr; gap:16px; padding:16px 20px; align-items:start; }
              @media (max-width:900px){ main { grid-template-columns:1fr; } }
              .panel { background:var(--panel); border:1px solid var(--line); border-radius:10px; overflow:hidden; }
              .panel h2 { font-size:12px; text-transform:uppercase; letter-spacing:.08em; color:var(--muted);
                          margin:0; padding:10px 14px; border-bottom:1px solid var(--line); }
              .toolbar { display:flex; gap:8px; padding:10px 14px; border-bottom:1px solid var(--line); flex-wrap:wrap; }
              select, input, button { background:#0d0f14; color:var(--fg); border:1px solid var(--line);
                                      border-radius:6px; padding:6px 8px; font:inherit; }
              button { cursor:pointer; }
              button.cancel { border-color:#5a2b2b; color:#ff9b9b; }
              table { width:100%; border-collapse:collapse; }
              th, td { text-align:left; padding:8px 14px; border-bottom:1px solid var(--line); font-size:13px;
                       white-space:nowrap; overflow:hidden; text-overflow:ellipsis; max-width:0; }
              tbody tr { cursor:pointer; }
              tbody tr:hover { background:#1e222b; }
              tbody tr.sel { background:#20293b; }
              .badge { font-size:11px; padding:2px 8px; border-radius:999px; border:1px solid var(--line); }
              .RUNNING{color:#6ea8ff;border-color:#2d4a80} .COMPLETED{color:#3ecf7a;border-color:#1f5a3a}
              .FAILED{color:#ff8080;border-color:#6a2b2b} .CANCELLED{color:#c9a86a;border-color:#5c4a24}
              .muted{color:var(--muted)} pre{margin:0;padding:12px 14px;white-space:pre-wrap;word-break:break-word;font-size:12px}
              .empty{padding:24px 14px;color:var(--muted)}
              code{color:var(--muted)}
            </style>
            </head>
            <body>
            <header>
              <h1>🌀 WIGGLE</h1>
              <span class="cluster" id="cluster">connecting…</span>
            </header>
            <main>
              <section class="panel" style="grid-column:1/-1">
                <h2>User tasks — awaiting completion</h2>
                <div id="tasks"><div class="empty">loading…</div></div>
              </section>
              <section class="panel">
                <h2>Instances</h2>
                <div class="toolbar">
                  <select id="workflow"><option value="">all workflows</option></select>
                  <select id="status">
                    <option value="">all statuses</option>
                    <option>RUNNING</option><option>COMPLETED</option><option>FAILED</option><option>CANCELLED</option>
                  </select>
                  <input id="limit" type="number" value="100" min="1" style="width:80px" title="limit">
                  <label class="muted" style="align-self:center"><input type="checkbox" id="auto" checked> auto</label>
                </div>
                <div id="rows"><div class="empty">loading…</div></div>
              </section>
              <section class="panel">
                <h2>Detail</h2>
                <div id="detail"><div class="empty">select an instance</div></div>
              </section>
            </main>
            <script>
            const $ = s => document.querySelector(s);
            let selected = null;

            async function j(url, opts) { const r = await fetch(url, opts); if(!r.ok) throw new Error((await r.json()).error||r.status); return r.json(); }
            const esc = s => (s==null?'':String(s)).replace(/[&<>]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));
            const ago = t => { if(!t) return ''; const s=Math.max(0,(Date.now()-t)/1000);
              return s<60?~~s+'s':s<3600?~~(s/60)+'m':~~(s/3600)+'h'; };

            async function loadCluster(){
              try { const c = await j('/api/cluster');
                const alive = c.members.filter(m=>m.alive).length;
                $('#cluster').innerHTML = c.members.map(m =>
                  `<span class="dot ${m.leader?'leader':''}"></span>${esc(m.name)}`).join(' &nbsp; ')
                  + ` &nbsp; <span class="muted">(${alive}/${c.members.length} alive)</span>`;
              } catch(e){ $('#cluster').textContent = 'cluster: '+e.message; }
            }

            async function loadWorkflows(){
              try { const w = await j('/api/workflows'); const sel = $('#workflow'); const cur = sel.value;
                sel.innerHTML = '<option value="">all workflows</option>' + w.workflows.map(n=>`<option>${esc(n)}</option>`).join('');
                sel.value = cur;
              } catch(e){}
            }

            async function loadInstances(){
              const wf = $('#workflow').value, st = $('#status').value, lim = $('#limit').value||100;
              const qs = new URLSearchParams(); if(wf)qs.set('workflow',wf); if(st)qs.set('status',st); qs.set('limit',lim);
              try {
                const d = await j('/api/instances?'+qs);
                if(!d.instances.length){ $('#rows').innerHTML='<div class="empty">no instances</div>'; return; }
                $('#rows').innerHTML = `<table><thead><tr><th>id</th><th>workflow</th><th>status</th><th>updated</th></tr></thead><tbody>`
                  + d.instances.map(i=>`<tr data-id="${esc(i.id)}" class="${i.id===selected?'sel':''}">
                      <td><code>${esc(i.id)}</code></td><td>${esc(i.workflow)}</td>
                      <td><span class="badge ${esc(i.status)}">${esc(i.status)}</span></td>
                      <td class="muted">${ago(i.updatedAt)} ago</td></tr>`).join('')
                  + `</tbody></table>`;
                document.querySelectorAll('#rows tr[data-id]').forEach(tr =>
                  tr.onclick = () => showDetail(tr.dataset.id));
              } catch(e){ $('#rows').innerHTML='<div class="empty">'+esc(e.message)+'</div>'; }
            }

            async function showDetail(id){
              selected = id;
              document.querySelectorAll('#rows tr').forEach(tr=>tr.classList.toggle('sel', tr.dataset.id===id));
              try {
                const d = await j('/api/instances/'+encodeURIComponent(id));
                const i = d.instance;
                const cancel = i.status==='RUNNING'
                  ? `<button class="cancel" onclick="cancelInstance('${esc(i.id)}')">cancel</button>` : '';
                const rows = d.tokens.map(t=>`<tr><td><code>${esc(t.nodeId)}</code></td><td>${esc(t.kind)}</td>
                    <td><span class="badge">${esc(t.status)}</span></td><td>${t.attempt}</td>
                    <td class="muted">${esc(t.lastError||'')}</td></tr>`).join('');
                $('#detail').innerHTML = `
                  <div class="toolbar"><span class="badge ${esc(i.status)}">${esc(i.status)}</span>
                    <code>${esc(i.id)}</code><span style="margin-left:auto">${cancel}</span></div>
                  ${i.error?`<pre style="color:#ff9b9b">${esc(i.error)}</pre>`:''}
                  ${i.terminationReason?`<div class="empty">reason: ${esc(i.terminationReason)}</div>`:''}
                  <h2>Context</h2><pre>${esc(JSON.stringify(i.context,null,2))}</pre>
                  <h2>Tokens</h2>
                  ${rows?`<table><thead><tr><th>node</th><th>kind</th><th>status</th><th>try</th><th>last error</th></tr></thead><tbody>${rows}</tbody></table>`:'<div class="empty">no tokens</div>'}`;
              } catch(e){ $('#detail').innerHTML='<div class="empty">'+esc(e.message)+'</div>'; }
            }

            async function cancelInstance(id){
              try { await j('/api/instances/'+encodeURIComponent(id)+'/cancel',{method:'POST'}); showDetail(id); loadInstances(); }
              catch(e){ alert('cancel failed: '+e.message); }
            }

            async function loadTasks(){
              try {
                const d = await j('/api/tasks');
                if(!d.tasks.length){ $('#tasks').innerHTML='<div class="empty">no pending user tasks</div>'; return; }
                $('#tasks').innerHTML = `<table><thead><tr><th>task</th><th>workflow</th><th>instance</th><th>deadline</th><th></th></tr></thead><tbody>`
                  + d.tasks.map(t=>`<tr><td>${esc(t.name)}</td><td>${esc(t.workflow)}</td>
                      <td><code>${esc(t.instanceId)}</code></td>
                      <td class="muted">${t.deadline?('in '+Math.max(0,Math.round((t.deadline-Date.now())/1000))+'s'):'—'}</td>
                      <td><button onclick="completeTask('${esc(t.id)}')">complete</button></td></tr>`).join('')
                  + `</tbody></table>`;
              } catch(e){ $('#tasks').innerHTML='<div class="empty">'+esc(e.message)+'</div>'; }
            }

            async function completeTask(id){
              const body = prompt('result JSON (merged into the context):', '{}');
              if(body===null) return;
              try {
                await j('/api/tasks/'+encodeURIComponent(id)+'/complete',
                        {method:'POST', headers:{'Content-Type':'application/json'}, body});
                loadTasks(); loadInstances();
              } catch(e){ alert('complete failed: '+e.message); }
            }

            function tick(){ loadInstances(); loadTasks(); if(selected) showDetail(selected); }
            ['#workflow','#status','#limit'].forEach(s=>$(s).onchange=loadInstances);
            loadWorkflows(); loadCluster(); loadInstances(); loadTasks();
            setInterval(()=>{ if($('#auto').checked) tick(); }, 2000);
            setInterval(()=>{ loadCluster(); loadWorkflows(); }, 5000);
            </script>
            </body>
            </html>
            """;
}
