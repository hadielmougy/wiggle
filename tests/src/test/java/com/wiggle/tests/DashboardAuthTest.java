package com.wiggle.tests;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import com.wiggle.server.http.DashboardAuth;
import com.wiggle.server.http.DashboardHttp;
import com.wiggle.server.store.InMemoryStorage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The dashboard's authentication is pluggable: a custom {@link DashboardAuth} injected via
 * {@code new WiggleServer(config, factory, auth)} fully replaces the built-in password login --
 * deciding who is authenticated, where to bounce a browser, what to tell the SPA, and registering
 * its own endpoints. This stands in for an SSO plugin living in a separate repository.
 */
class DashboardAuthTest {

    /** A trivial stand-in for SSO: a shared bearer-style header, its own login page, no Basic challenge. */
    static final class TokenAuth implements DashboardAuth {
        @Override public Optional<Principal> authenticate(HttpExchange ex) {
            return "let-me-in".equals(ex.getRequestHeaders().getFirst("X-Token"))
                    ? Optional.of(new Principal("sso-user")) : Optional.empty();
        }
        @Override public String loginLocation(HttpExchange ex) { return "/sso/login"; }
        @Override public String apiChallenge() { return null; }   // no Basic prompt
        @Override public Map<String, Object> describe(HttpExchange ex) { return Map.of("provider", "acme-sso"); }
        @Override public void install(HttpServer http) {
            http.createContext("/sso/login", DashboardHttp.guarded(
                    ex -> DashboardHttp.sendText(ex, 200, DashboardHttp.HTML, "<h1>acme-sso sign in</h1>")));
        }
    }

    @Test @DisplayName("a custom DashboardAuth replaces the built-in login end to end")
    void pluggableAuth() throws Exception {
        int dash = freePort();
        ServerConfig config = new ServerConfig(0, "dash-auth-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, dash,
                Duration.ofSeconds(5), Duration.ofSeconds(10));

        try (WiggleServer server = new WiggleServer(config, cfg -> new InMemoryStorage(), new TokenAuth()).start()) {
            String base = "http://localhost:" + server.dashboardPort();

            // Health check stays open regardless of the authenticator.
            assertEquals(200, get(base + "/healthz", null).statusCode());

            // An anonymous API call is rejected -- with NO Basic challenge, since this auth returns none.
            HttpResponse<String> anonApi = get(base + "/api/cluster", null);
            assertEquals(401, anonApi.statusCode());
            assertTrue(anonApi.headers().firstValue("WWW-Authenticate").isEmpty(), "no Basic challenge for token auth");

            // An anonymous browser is redirected to the plugin's own login location, not /login.
            HttpResponse<String> anonPage = get(base + "/", null);
            assertEquals(302, anonPage.statusCode());
            assertEquals("/sso/login", anonPage.headers().firstValue("Location").orElse(""));

            // The plugin's registered endpoint is reachable while unauthenticated.
            HttpResponse<String> loginPage = get(base + "/sso/login", null);
            assertEquals(200, loginPage.statusCode());
            assertTrue(loginPage.body().contains("acme-sso sign in"), "plugin serves its own login page");

            // Presenting the token authenticates the API call.
            HttpResponse<String> ok = get(base + "/api/cluster", "let-me-in");
            assertEquals(200, ok.statusCode());
            assertTrue(ok.body().contains("\"members\""));

            // /api/auth exposes the required flag plus whatever the plugin describes to the SPA.
            String authInfo = get(base + "/api/auth", null).body();
            assertTrue(authInfo.contains("\"required\":true"), "reports auth required");
            assertTrue(authInfo.contains("acme-sso"), "surfaces the plugin's describe() fields");
        }
    }

    private final HttpClient http = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build();

    private HttpResponse<String> get(String url, String token) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url)).GET();
        if (token != null) b.header("X-Token", token);
        return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) { return s.getLocalPort(); }
    }
}
