package dev.wiggle.server.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.util.Map;
import java.util.Optional;

/**
 * Pluggable authentication for the dashboard and its JSON API. Implement this to replace the
 * built-in admin-password login ({@link PasswordAuth}) with something else -- SSO / OIDC, a header
 * trusted from a reverse proxy, mTLS-only, ... -- typically in a separate (private) module, then
 * inject it via {@code new WiggleServer(config, storageFactory, myAuth)}.
 *
 * <p>The dashboard calls {@link #authenticate} on every guarded request. When it returns empty the
 * dashboard challenges an API caller with 401 (adding {@link #apiChallenge} if non-null) and
 * redirects a browser to {@link #loginLocation}. An implementation registers its own endpoints
 * (a login form, an OAuth callback, logout) from {@link #install}, using the helpers in
 * {@link DashboardHttp} to read and write. Only {@code /healthz} and {@code /api/auth} are served by
 * the dashboard itself and always open; everything else is guarded.
 *
 * <p>Implementations must be thread-safe: {@link #authenticate} is called concurrently on virtual
 * threads.
 */
public interface DashboardAuth {

    /** An authenticated caller. */
    record Principal(String name) {}

    /**
     * Decide whether this request is authenticated, e.g. from a session cookie, a bearer token, or
     * a client certificate.
     *
     * @return the caller's principal, or empty if the request is not authenticated
     */
    Optional<Principal> authenticate(HttpExchange exchange);

    /** Whether authentication is enforced at all. When {@code false} the dashboard is wide open. */
    default boolean required() { return true; }

    /**
     * Where to redirect an unauthenticated <em>browser</em> (a page request) -- the login form, or an
     * identity provider's authorize URL for SSO. Defaults to the built-in {@code /login} form.
     */
    default String loginLocation(HttpExchange exchange) { return "/login"; }

    /**
     * The {@code WWW-Authenticate} challenge for an unauthenticated <em>API</em> request, or
     * {@code null} for none. {@link PasswordAuth} returns a Basic challenge so {@code curl -u} works.
     */
    default String apiChallenge() { return null; }

    /**
     * Register any endpoints this authenticator needs on the dashboard's HTTP server -- a login
     * form/POST, an OAuth callback, logout. Called once at startup, before the server starts. Routes
     * added here are not guarded (they must be reachable while unauthenticated). Longest-prefix match
     * means a specific path like {@code /login} wins over the dashboard's catch-all {@code /}.
     */
    default void install(HttpServer http) {}

    /**
     * Extra fields to expose to the single-page app via {@code GET /api/auth}, merged with the
     * dashboard's {@code required} flag -- e.g. the signed-in user's name, or a login URL for the SPA
     * to link to.
     */
    default Map<String, Object> describe(HttpExchange exchange) { return Map.of(); }
}
