package com.wiggle.console;

import com.wiggle.client.DirectConnection;
import com.wiggle.client.WiggleConnection;
import com.wiggle.core.Tls;

import java.nio.file.Path;

/**
 * The standalone ops console: serves the dashboard SPA as a pure gRPC client of the cluster at
 * {@code WIGGLE_URL} (default {@code localhost:8080}).
 *
 * <p>Auth/TLS reuse the same {@code WIGGLE_DASHBOARD_*} / {@code WIGGLE_TLS_*} env as an embedded dashboard;
 * it serves on {@code WIGGLE_DASHBOARD_PORT} (default 8090).
 */
public final class ConsoleMain {

    public static void main(String[] args) throws Exception {
        Tls.Options tls = Tls.Options.fromEnvironment();
        String url = env("WIGGLE_URL", "localhost:8080");
        DirectConnection connection = WiggleConnection.direct(url, tls);

        int port = Integer.parseInt(env("WIGGLE_DASHBOARD_PORT", "8090"));
        // Optional read-only account: set WIGGLE_DASHBOARD_VIEWER_PASSWORD to add a viewer that can see
        // everything but can't cancel/signal/schedule. Only meaningful alongside the built-in admin.
        // Accounts an admin manages from the console live in this file, not in the control plane:
        // the gRPC API has no per-RPC authorization, so credentials there would be readable by every
        // worker. Mount it on a volume, or the accounts go when the container does.
        ConsoleUsers users = new ConsoleUsers(Path.of(env("WIGGLE_CONSOLE_USERS_FILE", "wiggle-users.json")));
        ConsoleAuth auth = new ConsoleAuth(env("WIGGLE_DASHBOARD_USER", "admin"),
                env("WIGGLE_DASHBOARD_PASSWORD", null),
                env("WIGGLE_DASHBOARD_VIEWER_USER", "viewer"),
                env("WIGGLE_DASHBOARD_VIEWER_PASSWORD", null),
                tls.hasKeyStore(), users);
        DashboardData data = new GrpcDashboardData(connection.client());
        ConsoleServer server = new ConsoleServer(data, auth, port, tls).start();

        System.out.println("wiggle console on " + (tls.hasKeyStore() ? "https" : "http") + "://localhost:"
                + server.port() + "  ->  " + url);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.close();
            connection.close();
        }));
        Thread.currentThread().join();
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? def : v;
    }

    private ConsoleMain() {}
}
