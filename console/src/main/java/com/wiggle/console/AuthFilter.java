package com.wiggle.console;

import com.wiggle.server.auth.Permissions;
import com.wiggle.server.store.StorageException;
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.Set;

/**
 * Guards every request. Authentication: an unauthenticated API call gets 401 (with the Basic challenge
 * for curl); an unauthenticated browser hitting a page is redirected to {@code /login}. Authorization:
 * every {@code /api/*} call needs the action {@link #action} names, on at least one scope; the servlet
 * then checks the scope the call touches. A few endpoints are always open, and a few are
 * self-service: signed in is enough, because they act on the caller's own account.
 */
public final class AuthFilter implements Filter {

    private static final Set<String> OPEN = Set.of("/api/auth", "/api/login", "/login", "/logout", "/healthz",
            "/setup", "/api/setup");
    /** Writes any signed-in account may make, because they change nothing but its own account. */
    private static final Set<String> SELF_SERVICE = Set.of("/api/password");
    private static final Set<String> READ_METHODS = Set.of("GET", "HEAD", "OPTIONS");

    private final ConsoleAuth auth;

    public AuthFilter(ConsoleAuth auth) {
        this.auth = auth;
    }

    /**
     * The action an {@code /api/*} call needs. A write not listed here needs {@value Permissions#ALL},
     * so a new mutating endpoint is locked down until it is given its own action.
     */
    static String action(String method, String path) {
        if (path.startsWith("/api/users") || path.startsWith("/api/roles") || path.startsWith("/api/audit")
                || path.startsWith("/api/credentials")) {
            return Permissions.USER_MANAGE;
        }
        if (READ_METHODS.contains(method)) return Permissions.READ;
        if (path.startsWith("/api/instances/") && path.endsWith("/cancel")) return Permissions.INSTANCE_CANCEL;
        if (path.startsWith("/api/instances/") && path.contains("/signal/")) return Permissions.INSTANCE_SIGNAL;
        if (path.startsWith("/api/schedules")) return Permissions.SCHEDULE_WRITE;
        return Permissions.ALL;
    }

    @Override
    public void doFilter(ServletRequest sreq, ServletResponse sres, FilterChain chain)
            throws IOException, ServletException {
        HttpServletRequest req = (HttpServletRequest) sreq;
        HttpServletResponse res = (HttpServletResponse) sres;
        String path = req.getRequestURI();
        if (OPEN.contains(path)) {
            chain.doFilter(sreq, sres);
            return;
        }
        // First run: until the admin's password is set, that screen is all there is.
        if (auth.setupRequired()) {
            if (path.startsWith("/api/")) {
                res.sendError(401, "the portal is not set up: set the admin password at /setup");
            } else {
                res.sendRedirect("/setup");
            }
            return;
        }
        ConsoleAuth.Principal principal;
        try {
            principal = auth.principal(req);
        } catch (StorageException e) {
            res.sendError(503, "the auth shard is unreachable; try again shortly");
            return;
        }
        if (principal == null) {
            if (path.startsWith("/api/")) {
                String challenge = auth.apiChallenge();
                if (challenge != null) res.setHeader("WWW-Authenticate", challenge);
                res.sendError(401, "authentication required");
            } else {
                res.sendRedirect("/login");
            }
            return;
        }
        if (path.startsWith("/api/") && !SELF_SERVICE.contains(path)) {
            String action = action(req.getMethod(), path);
            boolean allowed = switch (action) {
                case Permissions.ALL -> principal.permissions().contains(Permissions.ALL);
                // The portal lists across workflows, so a read scoped to some of them is not enough.
                // Search narrows its hits to what the caller may read, so a scoped read is enough there.
                case Permissions.READ -> path.equals("/api/search")
                        ? principal.allowsAny(Permissions.READ) : principal.allows(Permissions.READ, null);
                default -> principal.allowsAny(action);
            };
            if (!allowed) {
                res.sendError(403, "permission '" + action + "' required");
                return;
            }
        }
        chain.doFilter(sreq, sres);
    }
}
