package dev.wiggle.server.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static dev.wiggle.server.http.DashboardHttp.constantTimeEquals;
import static dev.wiggle.server.http.DashboardHttp.readJsonBody;
import static dev.wiggle.server.http.DashboardHttp.redirect;
import static dev.wiggle.server.http.DashboardHttp.sendError;
import static dev.wiggle.server.http.DashboardHttp.sendJson;
import static dev.wiggle.server.http.DashboardHttp.sendText;

/**
 * The built-in {@link DashboardAuth}: a single admin account, authenticated by either a session
 * cookie (from the {@code /login} form) or HTTP Basic credentials (for {@code curl}/programmatic
 * use). With no password the dashboard is unauthenticated ({@link #required()} is false) -- front it
 * with TLS or a private network in that case, since credentials travel in cleartext over plain HTTP.
 *
 * <p>Sessions are held per node (nodes don't share them). This is the class to look at as a worked
 * example when writing a custom authenticator.
 */
public final class PasswordAuth implements DashboardAuth {

    private static final String SESSION_COOKIE = "wiggle_session";
    private static final long SESSION_TTL_MILLIS = 12 * 60 * 60 * 1000L;   // 12 hours

    private final String user;
    private final String password;
    private final boolean secureCookies;
    private final SecureRandom random = new SecureRandom();
    /** Live session tokens -> expiry epoch millis. */
    private final Map<String, Long> sessions = new ConcurrentHashMap<>();

    /**
     * @param user          admin username; blank defaults to {@code admin}
     * @param password      admin password; blank/null leaves the dashboard unauthenticated
     * @param secureCookies mark the session cookie {@code Secure} (set this when serving over TLS)
     */
    public PasswordAuth(String user, String password, boolean secureCookies) {
        this.user = user == null || user.isBlank() ? "admin" : user;
        this.password = password == null || password.isBlank() ? null : password;
        this.secureCookies = secureCookies;
    }

    @Override public boolean required() { return password != null; }

    @Override public String apiChallenge() {
        return password != null ? "Basic realm=\"Wiggle\", charset=\"UTF-8\"" : null;
    }

    @Override public Optional<Principal> authenticate(HttpExchange ex) {
        if (password == null) return Optional.of(new Principal(user));   // unauthenticated mode
        boolean ok = validSession(ex) || validBasic(ex.getRequestHeaders().getFirst("Authorization"));
        return ok ? Optional.of(new Principal(user)) : Optional.empty();
    }

    @Override public Map<String, Object> describe(HttpExchange ex) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("user", password != null ? user : null);
        return m;
    }

    @Override public void install(HttpServer http) {
        http.createContext("/login", DashboardHttp.guarded(this::loginPage));    // open: the login form
        http.createContext("/api/login", DashboardHttp.guarded(this::login));    // open: establishes a session
        http.createContext("/logout", DashboardHttp.guarded(this::logout));      // clears the session
    }

    /** GET /login: the form (or a bounce to the app if already in, or if auth is off). */
    private void loginPage(HttpExchange ex) throws IOException {
        if (!required() || authenticate(ex).isPresent()) { redirect(ex, "/"); return; }
        sendText(ex, 200, DashboardHttp.HTML, LOGIN_HTML);
    }

    /** POST {user, password}: on match, mints a session cookie; otherwise 401. */
    private void login(HttpExchange ex) throws IOException {
        if (!ex.getRequestMethod().equals("POST")) { sendError(ex, 405, "POST required"); return; }
        Map<String, Object> body = dev.wiggle.core.Json.asObject(readJsonBody(ex));
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
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    private String sessionCookie(String token, long maxAgeSeconds) {
        return SESSION_COOKIE + "=" + token + "; Max-Age=" + maxAgeSeconds
                + "; Path=/; HttpOnly; SameSite=Strict" + (secureCookies ? "; Secure" : "");
    }

    /** True iff {@code header} is {@code Basic <base64(user:password)>} matching the admin account. */
    private boolean validBasic(String header) {
        if (header == null || !header.regionMatches(true, 0, "Basic ", 0, 6)) return false;
        String decoded;
        try {
            decoded = new String(Base64.getDecoder().decode(header.substring(6).trim()), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException badBase64) {
            return false;
        }
        int colon = decoded.indexOf(':');
        if (colon < 0) return false;
        // Constant-time on both fields so a match can't be timed out character by character.
        boolean userOk = constantTimeEquals(decoded.substring(0, colon), user);
        boolean passOk = constantTimeEquals(decoded.substring(colon + 1), password);
        return userOk & passOk;
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
