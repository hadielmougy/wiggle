package com.wiggle.console;

import jakarta.servlet.http.HttpServletRequest;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The console's built-in auth: a single admin account by session cookie (from the {@code /login} form)
 * or HTTP Basic (for {@code curl}). With no password the console is unauthenticated. Ported from the
 * cell dashboard's {@code PasswordAuth} to the servlet API; sessions are per process.
 */
final class ConsoleAuth {

    static final String SESSION_COOKIE = "wiggle_session";
    private static final long SESSION_TTL_MILLIS = 12 * 60 * 60 * 1000L;

    private final String user;
    private final String password;
    private final boolean secureCookies;
    private final SecureRandom random = new SecureRandom();
    private final Map<String, Long> sessions = new ConcurrentHashMap<>();

    ConsoleAuth(String user, String password, boolean secureCookies) {
        this.user = user == null || user.isBlank() ? "admin" : user;
        this.password = password == null || password.isBlank() ? null : password;
        this.secureCookies = secureCookies;
    }

    boolean required() { return password != null; }

    String user() { return user; }

    String apiChallenge() {
        return password != null ? "Basic realm=\"Wiggle\", charset=\"UTF-8\"" : null;
    }

    boolean authenticated(HttpServletRequest req) {
        if (password == null) return true;   // unauthenticated mode
        return validSession(req) || validBasic(req.getHeader("Authorization"));
    }

    /** On matching credentials, mints a session and returns the {@code Set-Cookie} value; else null. */
    String login(String u, String p) {
        if (password == null || !eq(u, user) || !eq(p, password)) return null;
        String token = newToken();
        sessions.put(token, System.currentTimeMillis() + SESSION_TTL_MILLIS);
        return cookie(token, SESSION_TTL_MILLIS / 1000);
    }

    void logout(HttpServletRequest req) {
        String token = sessionToken(req);
        if (token != null) sessions.remove(token);
    }

    String expiredCookie() { return cookie("", 0); }

    private boolean validSession(HttpServletRequest req) {
        String token = sessionToken(req);
        if (token == null) return false;
        Long expiry = sessions.get(token);
        if (expiry == null) return false;
        if (expiry < System.currentTimeMillis()) { sessions.remove(token); return false; }
        return true;
    }

    private static String sessionToken(HttpServletRequest req) {
        String header = req.getHeader("Cookie");
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
        boolean userOk = eq(decoded.substring(0, colon), user);
        boolean passOk = eq(decoded.substring(colon + 1), password);
        return userOk & passOk;
    }

    private String newToken() {
        byte[] bytes = new byte[32];
        random.nextBytes(bytes);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    private String cookie(String token, long maxAgeSeconds) {
        return SESSION_COOKIE + "=" + token + "; Max-Age=" + maxAgeSeconds
                + "; Path=/; HttpOnly; SameSite=Strict" + (secureCookies ? "; Secure" : "");
    }

    /** Constant-time equality so a match can't be timed out character by character. */
    private static boolean eq(String a, String b) {
        if (a == null || b == null) return false;
        return MessageDigest.isEqual(a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
    }

    static final String LOGIN_HTML = """
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
                <p class="sub">Sign in to the console</p>
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
