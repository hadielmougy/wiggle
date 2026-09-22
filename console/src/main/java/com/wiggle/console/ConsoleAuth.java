package com.wiggle.console;

import jakarta.servlet.http.HttpServletRequest;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The console's auth, by session cookie (from the {@code /login} form) or HTTP Basic (for
 * {@code curl}). Authentication answers "who are you"; authorization is one bit —
 * {@link Role#ADMIN} may mutate (cancel / signal / schedules / users), {@link Role#VIEWER} is
 * read-only.
 *
 * <p>Two sources of accounts. The environment configures up to two <b>built-in</b> accounts
 * ({@code WIGGLE_DASHBOARD_PASSWORD} and the optional viewer), which no one can change from the
 * running console. On top of those, an admin manages accounts in a {@link ConsoleUsers} file,
 * and any account can change its own password.
 *
 * <p>With neither a built-in password nor a managed account the console is unauthenticated and
 * every request is an admin (open mode); creating the first managed account therefore turns
 * authentication on. Sessions are per process.
 */
final class ConsoleAuth {

    static final String SESSION_COOKIE = "wiggle_session";
    private static final long SESSION_TTL_MILLIS = 12 * 60 * 60 * 1000L;

    /** Access level: ADMIN has full read/write, VIEWER is read-only. */
    enum Role {
        ADMIN, VIEWER;

        /** The name this role travels under, in the user file and the JSON API. */
        String wire() { return name().toLowerCase(); }

        /** Parses a role name; {@code operator} is the old name for {@link #ADMIN}. */
        static Role of(String name) {
            String v = name == null ? "" : name.trim().toLowerCase();
            return switch (v) {
                case "admin", "operator" -> ADMIN;
                case "viewer" -> VIEWER;
                default -> throw new IllegalArgumentException("role is 'admin' or 'viewer', not '" + name + "'");
            };
        }
    }

    private final String user;
    private final String password;
    private final String viewerUser;
    private final String viewerPassword;
    private final boolean secureCookies;
    private final ConsoleUsers users;
    private final SecureRandom random = new SecureRandom();
    private final Map<String, Session> sessions = new ConcurrentHashMap<>();

    private record Session(long expiry, String user, Role role) {}

    /** Admin-only console (no read-only viewer account), with no managed users. */
    ConsoleAuth(String user, String password, boolean secureCookies) {
        this(user, password, null, null, secureCookies, null);
    }

    ConsoleAuth(String user, String password, String viewerUser, String viewerPassword, boolean secureCookies) {
        this(user, password, viewerUser, viewerPassword, secureCookies, null);
    }

    ConsoleAuth(String user, String password, String viewerUser, String viewerPassword, boolean secureCookies,
                ConsoleUsers users) {
        this.users = users;
        this.user = user == null || user.isBlank() ? "admin" : user;
        this.password = password == null || password.isBlank() ? null : password;
        this.viewerUser = viewerUser == null || viewerUser.isBlank() ? "viewer" : viewerUser;
        // A built-in viewer only exists alongside the built-in admin; ignored without it.
        this.viewerPassword = this.password == null || viewerPassword == null || viewerPassword.isBlank()
                ? null : viewerPassword;
        this.secureCookies = secureCookies;
    }

    /** Whether anyone must sign in: a built-in password, or any managed account, turns auth on. */
    boolean required() { return password != null || (users != null && !users.isEmpty()); }

    /** The built-in admin's name, which is also the login form's default. */
    String user() { return user; }

    /** The managed accounts, or null when this console has no user file. */
    ConsoleUsers users() { return users; }

    /** The names no managed account may take, because a built-in already answers to them. */
    Set<String> builtinNames() {
        Set<String> names = new LinkedHashSet<>();
        if (password != null) names.add(user);
        if (viewerPassword != null) names.add(viewerUser);
        return names;
    }

    /** Whether a built-in admin can still sign in; false means managed admins are the only way in. */
    boolean hasBuiltinAdmin() { return password != null; }

    String apiChallenge() {
        return required() ? "Basic realm=\"Wiggle\", charset=\"UTF-8\"" : null;
    }

    /** The caller's role, or null if authentication is required and they aren't authenticated. */
    Role role(HttpServletRequest req) {
        if (!required()) return Role.ADMIN;   // open mode: everyone is an admin
        Session s = session(req);
        if (s != null) return s.role();
        Credentials c = basic(req.getHeader("Authorization"));
        return c == null ? null : credentialRole(c.user(), c.password());
    }

    /** The caller's account name, or null when unauthenticated or in open mode. */
    String signedInUser(HttpServletRequest req) {
        if (!required()) return null;
        Session s = session(req);
        if (s != null) return s.user();
        Credentials c = basic(req.getHeader("Authorization"));
        return c != null && credentialRole(c.user(), c.password()) != null ? c.user() : null;
    }

    boolean authenticated(HttpServletRequest req) {
        return role(req) != null;
    }

    /** Whether the caller may perform mutating operations (cancel / signal / schedules / users). */
    boolean canWrite(HttpServletRequest req) {
        return role(req) == Role.ADMIN;
    }

    /**
     * Changes the caller's own password, given their current one. Built-in accounts come from the
     * environment and cannot be changed here. Every other session of that account is dropped; the
     * caller keeps the one they are using.
     */
    void changeOwnPassword(HttpServletRequest req, String current, String next) {
        String name = signedInUser(req);
        if (name == null) throw new IllegalStateException("not signed in");
        if (builtinNames().contains(name)) {
            throw new IllegalArgumentException("'" + name + "' is a built-in account set in the environment; "
                    + "change WIGGLE_DASHBOARD_PASSWORD where the console is deployed, not here");
        }
        requireUsers();
        if (users.verify(name, current) == null) {
            throw new IllegalArgumentException("the current password is wrong");
        }
        users.setPassword(name, next, System.currentTimeMillis());
        revokeSessions(name, sessionToken(req));
    }

    /** Drops every session of {@code name}, except {@code keepToken} when it is non-null. */
    void revokeSessions(String name, String keepToken) {
        sessions.entrySet().removeIf(e -> e.getValue().user().equals(name) && !e.getKey().equals(keepToken));
    }

    ConsoleUsers requireUsers() {
        if (users == null) {
            throw new IllegalStateException("this console manages no users: it was built without a user file");
        }
        return users;
    }

    /** On matching credentials, mints a session bound to the matched role and returns the {@code
     * Set-Cookie} value; else null. */
    String login(String u, String p) {
        Role role = credentialRole(u, p);
        if (role == null) return null;
        String token = newToken();
        sessions.put(token, new Session(System.currentTimeMillis() + SESSION_TTL_MILLIS, u, role));
        return cookie(token, SESSION_TTL_MILLIS / 1000);
    }

    void logout(HttpServletRequest req) {
        String token = sessionToken(req);
        if (token != null) sessions.remove(token);
    }

    String expiredCookie() { return cookie("", 0); }

    /** Which role these credentials authenticate as, built-ins first, or null if none match. */
    private Role credentialRole(String u, String p) {
        if (password != null && eq(u, user) && eq(p, password)) return Role.ADMIN;
        if (viewerPassword != null && eq(u, viewerUser) && eq(p, viewerPassword)) return Role.VIEWER;
        return users == null ? null : users.verify(u, p);
    }

    private record Credentials(String user, String password) {}

    private static Credentials basic(String header) {
        if (header == null || !header.regionMatches(true, 0, "Basic ", 0, 6)) return null;
        String decoded;
        try {
            decoded = new String(Base64.getDecoder().decode(header.substring(6).trim()), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException badBase64) {
            return null;
        }
        int colon = decoded.indexOf(':');
        if (colon < 0) return null;
        return new Credentials(decoded.substring(0, colon), decoded.substring(colon + 1));
    }

    private Session session(HttpServletRequest req) {
        String token = sessionToken(req);
        if (token == null) return null;
        Session s = sessions.get(token);
        if (s == null) return null;
        if (s.expiry() < System.currentTimeMillis()) { sessions.remove(token); return null; }
        return s;
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
