package com.wiggle.console;

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
 * for curl); an unauthenticated browser hitting a page is redirected to {@code /login}. Authorization: a
 * read-only viewer that tries to mutate (any non-GET {@code /api/*} call -- cancel, signal, schedule
 * changes) gets 403. A few endpoints are always open. Mirrors the cell dashboard's {@code securedApi} /
 * {@code securedPage} split, as a servlet filter.
 */
public final class AuthFilter implements Filter {

    private static final Set<String> OPEN = Set.of("/api/auth", "/api/login", "/login", "/logout", "/healthz");
    private static final Set<String> READ_METHODS = Set.of("GET", "HEAD", "OPTIONS");

    private final ConsoleAuth auth;

    public AuthFilter(ConsoleAuth auth) {
        this.auth = auth;
    }

    @Override
    public void doFilter(ServletRequest sreq, ServletResponse sres, FilterChain chain)
            throws IOException, ServletException {
        HttpServletRequest req = (HttpServletRequest) sreq;
        HttpServletResponse res = (HttpServletResponse) sres;
        String path = req.getRequestURI();
        boolean open = !auth.required() || OPEN.contains(path);

        if (!open && !auth.authenticated(req)) {
            if (path.startsWith("/api/")) {
                String challenge = auth.apiChallenge();
                if (challenge != null) res.setHeader("WWW-Authenticate", challenge);
                res.sendError(401, "authentication required");
            } else {
                res.sendRedirect("/login");
            }
            return;
        }
        // Authenticated (or an open path): a viewer may read but not mutate. Any non-GET /api call is a
        // write; default-deny keeps new mutating endpoints locked down without touching this filter.
        if (!open && path.startsWith("/api/") && !READ_METHODS.contains(req.getMethod()) && !auth.canWrite(req)) {
            res.sendError(403, "read-only: operator role required");
            return;
        }
        chain.doFilter(sreq, sres);
    }
}
