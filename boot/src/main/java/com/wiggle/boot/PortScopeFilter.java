package com.wiggle.boot;

import com.wiggle.core.wire.ControlPlane;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/**
 * Keeps each port to its own paths: the API port serves only the API's, and the portal port everything
 * but those. Anything else is 404.
 */
final class PortScopeFilter extends OncePerRequestFilter {

    private final int apiPort;
    private final int portalPort;

    PortScopeFilter(int apiPort, int portalPort) {
        this.apiPort = apiPort;
        this.portalPort = portalPort;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest req, HttpServletResponse resp, FilterChain chain)
            throws ServletException, IOException {
        boolean api = req.getRequestURI().startsWith(ControlPlane.PREFIX);
        int port = req.getLocalPort();
        if ((port == apiPort && !api) || (port == portalPort && api)) {
            resp.sendError(HttpServletResponse.SC_NOT_FOUND);
            return;
        }
        chain.doFilter(req, resp);
    }
}
