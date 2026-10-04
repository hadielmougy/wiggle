package com.wiggle.console;

import com.wiggle.server.auth.Accounts;
import com.wiggle.server.auth.AuthCache;
import com.wiggle.server.auth.Permissions;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.StorageException;
import jakarta.servlet.http.HttpServletRequest;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The portal's auth, by session cookie (from the {@code /login} form) or HTTP Basic (for
 * {@code curl}). Authentication answers "who are you" with a {@link Principal}; what it may do is
 * its permission set ({@link Permissions}).
 *
 * <p>Two sources of accounts. The environment configures up to two <b>built-in</b> accounts
 * ({@code WIGGLE_DASHBOARD_PASSWORD} as {@code admin} and the optional viewer), which no one can
 * change from the running portal and which sign in even when the auth shard is down. On top of
 * those, accounts and roles managed in the portal live on the auth shard ({@link Accounts}), read
 * through this node's {@link AuthCache}. Sessions live there too, so any portal node serves any
 * signed-in request; a built-in account signing in while the auth shard is unreachable gets a
 * session held by this node only.
 *
 * <p>With neither a built-in password nor a managed account the portal is unauthenticated and every
 * request may do anything (open mode); creating the first managed account turns authentication on.
 */
final class ConsoleAuth {

    static final String SESSION_COOKIE = "wiggle_session";
    static final String PRINCIPAL_ATTRIBUTE = "wiggle.principal";
    private static final long SESSION_TTL_MILLIS = 12 * 60 * 60 * 1000L;

    /** Who a request is, and what it may do. {@code user} is null in open mode. */
    record Principal(String user, Set<String> permissions, boolean builtin) {

        boolean allows(String action, String scope) {
            return Permissions.allows(permissions, action, scope);
        }

        /** Whether it may do {@code action} on at least one scope. */
        boolean allowsAny(String action) {
            if (permissions.contains(Permissions.ALL) || permissions.contains(action)) return true;
            return permissions.stream().anyMatch(p -> p.startsWith(action + ":"));
        }

        /** Whether it may do anything beyond reading. */
        boolean writes() {
            return permissions.stream().anyMatch(p -> !p.equals(Permissions.PORTAL_READ));
        }
    }

    private static final Principal OPEN = new Principal(null, Set.of(Permissions.ALL), false);

    private final String user;
    private final String password;
    private final String viewerUser;
    private final String viewerPassword;
    private final boolean secureCookies;
    private final Accounts accounts;
    private final AuthCache cache;
    private final SecureRandom random = new SecureRandom();
    /** Sessions of built-in accounts opened while the auth shard was unreachable. */
    private final Map<String, LocalSession> localSessions = new ConcurrentHashMap<>();

    private record LocalSession(long expiry, String user) {}

    /** Admin-only portal (no read-only viewer account), with no managed accounts. */
    ConsoleAuth(String user, String password, boolean secureCookies) {
        this(user, password, null, null, secureCookies, null, null);
    }

    ConsoleAuth(String user, String password, String viewerUser, String viewerPassword, boolean secureCookies) {
        this(user, password, viewerUser, viewerPassword, secureCookies, null, null);
    }

    ConsoleAuth(String user, String password, String viewerUser, String viewerPassword, boolean secureCookies,
                Accounts accounts, AuthCache cache) {
        this.accounts = accounts;
        this.cache = cache;
        this.user = user == null || user.isBlank() ? "admin" : user;
        this.password = password == null || password.isBlank() ? null : password;
        this.viewerUser = viewerUser == null || viewerUser.isBlank() ? "viewer" : viewerUser;
        // A built-in viewer only exists alongside the built-in admin; ignored without it.
        this.viewerPassword = this.password == null || viewerPassword == null || viewerPassword.isBlank()
                ? null : viewerPassword;
        this.secureCookies = secureCookies;
    }

    /**
     * Whether anyone must sign in: a built-in password, or any managed account, turns auth on. When
     * the auth shard cannot say, it is required: an outage never opens the portal.
     */
    boolean required() {
        if (password != null) return true;
        if (cache == null) return false;
        try {
            return cache.anyAccount();
        } catch (StorageException e) {
            return true;
        }
    }

    /** The built-in admin's name, which is also the login form's default. */
    String user() { return user; }

    /** The managed accounts, or null when this portal has none. */
    Accounts accounts() { return accounts; }

    /** The names no managed account may take, because a built-in already answers to them. */
    Set<String> builtinNames() {
        Set<String> names = new LinkedHashSet<>();
        if (password != null) names.add(user);
        if (viewerPassword != null) names.add(viewerUser);
        return names;
    }

    /** Whether a built-in admin can still sign in; false means managed accounts are the only way in. */
    boolean hasBuiltinAdmin() { return password != null; }

    String apiChallenge() {
        return required() ? "Basic realm=\"Wiggle\", charset=\"UTF-8\"" : null;
    }

    /** The caller, or null if authentication is required and they aren't authenticated. */
    Principal principal(HttpServletRequest req) {
        if (req.getAttribute(PRINCIPAL_ATTRIBUTE) instanceof Principal p) return p;
        Principal p = resolve(req);
        if (p != null) req.setAttribute(PRINCIPAL_ATTRIBUTE, p);
        return p;
    }

    private Principal resolve(HttpServletRequest req) {
        if (!required()) return OPEN;
        String token = sessionToken(req);
        if (token != null) {
            Principal p = sessionPrincipal(token);
            if (p != null) return p;
        }
        Credentials c = basic(req.getHeader("Authorization"));
        return c == null ? null : credentialPrincipal(c.user(), c.password());
    }

    /** The caller's account name, or null when unauthenticated or in open mode. */
    String signedInUser(HttpServletRequest req) {
        Principal p = principal(req);
        return p == null ? null : p.user();
    }

    boolean authenticated(HttpServletRequest req) {
        return principal(req) != null;
    }

    /**
     * Changes the caller's own password, given their current one. Built-in accounts come from the
     * environment and cannot be changed here. Every other session of that account is ended; the
     * caller keeps the one they are using.
     */
    void changeOwnPassword(HttpServletRequest req, String current, String next) {
        String name = signedInUser(req);
        if (name == null) throw new IllegalStateException("not signed in");
        if (builtinNames().contains(name)) {
            throw new IllegalArgumentException("'" + name + "' is a built-in account set in the environment; "
                    + "change WIGGLE_DASHBOARD_PASSWORD where the server is deployed, not here");
        }
        Accounts a = requireAccounts();
        Optional<Accounts.Account> account = a.account(name);
        if (account.isEmpty() || !account.get().passwordMatches(current)) {
            throw new IllegalArgumentException("the current password is wrong");
        }
        a.setPassword(name, name, next, sessionToken(req));
        cache.forgetUser(name);
    }

    Accounts requireAccounts() {
        if (accounts == null) {
            throw new IllegalStateException("this portal manages no accounts");
        }
        return accounts;
    }

    /**
     * On matching credentials, opens a session and returns its {@code Set-Cookie} value; else null.
     * A managed account's password is checked against the auth primary, so a sign-in fails while
     * the auth shard is unreachable.
     */
    String login(String u, String p) {
        Principal builtin = builtinPrincipal(u, p);
        if (builtin != null) {
            String token;
            try {
                token = accounts == null ? localSession(u) : accounts.openSession(u, SESSION_TTL_MILLIS);
            } catch (StorageException e) {
                token = localSession(u);
            }
            return cookie(token, SESSION_TTL_MILLIS / 1000);
        }
        if (accounts == null || u == null) return null;
        Optional<Accounts.Account> account = accounts.account(u);
        if (account.isEmpty() || account.get().disabled() || !account.get().passwordMatches(p)) return null;
        return cookie(accounts.openSession(u, SESSION_TTL_MILLIS), SESSION_TTL_MILLIS / 1000);
    }

    private String localSession(String u) {
        String token = newToken();
        localSessions.put(token, new LocalSession(System.currentTimeMillis() + SESSION_TTL_MILLIS, u));
        return token;
    }

    void logout(HttpServletRequest req) {
        String token = sessionToken(req);
        if (token == null) return;
        if (localSessions.remove(token) != null || accounts == null) return;
        accounts.closeSession(token);
        cache.forgetSession(Accounts.tokenHash(token));
    }

    String expiredCookie() { return cookie("", 0); }

    private Principal sessionPrincipal(String token) {
        long now = System.currentTimeMillis();
        LocalSession local = localSessions.get(token);
        if (local != null) {
            if (local.expiry() < now) { localSessions.remove(token); return null; }
            return builtinFor(local.user());
        }
        if (cache == null) return null;
        Rows.AuthSession s = cache.session(Accounts.tokenHash(token)).orElse(null);
        if (s == null || s.expiresAt() < now) return null;
        if (builtinNames().contains(s.user())) return builtinFor(s.user());
        return cache.account(s.user()).filter(a -> !a.disabled())
                .map(a -> new Principal(a.name(), a.permissions(), false)).orElse(null);
    }

    /** Which principal these credentials authenticate as, built-ins first, or null if none match. */
    private Principal credentialPrincipal(String u, String p) {
        Principal builtin = builtinPrincipal(u, p);
        if (builtin != null) return builtin;
        if (cache == null || u == null) return null;
        return cache.account(u).filter(a -> !a.disabled() && a.passwordMatches(p))
                .map(a -> new Principal(a.name(), a.permissions(), false)).orElse(null);
    }

    private Principal builtinPrincipal(String u, String p) {
        if (password != null && eq(u, user) && eq(p, password)) return builtinFor(user);
        if (viewerPassword != null && eq(u, viewerUser) && eq(p, viewerPassword)) return builtinFor(viewerUser);
        return null;
    }

    /** The built-in account {@code name} as a principal, or null when it is no longer configured. */
    private Principal builtinFor(String name) {
        if (password != null && name.equals(user)) {
            return new Principal(name, Permissions.BUILTIN_ROLES.get(Permissions.ADMIN), true);
        }
        if (viewerPassword != null && name.equals(viewerUser)) {
            return new Principal(name, Permissions.BUILTIN_ROLES.get(Permissions.VIEWER), true);
        }
        return null;
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

    static String sessionToken(HttpServletRequest req) {
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
                <p class="sub">Sign in to the portal</p>
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
