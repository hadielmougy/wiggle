package com.wiggle.console;

import com.wiggle.core.Tls;
import com.wiggle.server.WiggleServer;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

/**
 * The portal: the dashboard SPA and its JSON API, served by a server process over its own engine
 * on {@code WIGGLE_PORTAL_PORT}, separate from the gRPC port.
 *
 * <p>Accounts come from {@code WIGGLE_DASHBOARD_USER} / {@code WIGGLE_DASHBOARD_PASSWORD}, the
 * optional read-only {@code WIGGLE_DASHBOARD_VIEWER_*} pair, and the file at
 * {@code WIGGLE_CONSOLE_USERS_FILE}. Sessions are held by the node that signed them in.
 */
public final class Portal implements AutoCloseable {

    public static final String PORT_ENV = "WIGGLE_PORTAL_PORT";

    private final ConsoleServer http;

    private Portal(ConsoleServer http) {
        this.http = http;
    }

    /** Starts the portal for {@code server} when {@code env} sets a positive {@value #PORT_ENV}. */
    public static Optional<Portal> fromEnvironment(WiggleServer server, Tls.Options tls, Map<String, String> env) {
        String raw = env.get(PORT_ENV);
        int port;
        try {
            port = raw == null || raw.isBlank() ? 0 : Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(PORT_ENV + "='" + raw + "' is not a port number");
        }
        if (port <= 0) return Optional.empty();
        ConsoleUsers users = new ConsoleUsers(Path.of(get(env, "WIGGLE_CONSOLE_USERS_FILE", "wiggle-users.json")));
        ConsoleAuth auth = new ConsoleAuth(get(env, "WIGGLE_DASHBOARD_USER", "admin"),
                get(env, "WIGGLE_DASHBOARD_PASSWORD", null),
                get(env, "WIGGLE_DASHBOARD_VIEWER_USER", "viewer"),
                get(env, "WIGGLE_DASHBOARD_VIEWER_PASSWORD", null),
                tls.hasKeyStore(), users);
        return Optional.of(start(server, auth, port, tls));
    }

    /** Starts the portal for {@code server} on {@code port}; 0 picks a free one. */
    static Portal start(WiggleServer server, ConsoleAuth auth, int port, Tls.Options tls) {
        DashboardData data = new EngineDashboardData(server.engine(), server.cluster());
        return new Portal(new ConsoleServer(data, auth, port, tls).start());
    }

    /** The port it is listening on. */
    public int port() {
        return http.port();
    }

    @Override public void close() {
        http.close();
    }

    private static String get(Map<String, String> env, String key, String def) {
        String v = env.get(key);
        return v == null || v.isBlank() ? def : v;
    }
}
