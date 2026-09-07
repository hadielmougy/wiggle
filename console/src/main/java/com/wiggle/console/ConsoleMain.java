package com.wiggle.console;

import com.wiggle.client.CoordinatedConnection;
import com.wiggle.client.DirectConnection;
import com.wiggle.client.WiggleConnection;
import com.wiggle.core.Tls;
import com.wiggle.server.http.DashboardData;
import com.wiggle.server.http.HttpDashboard;

/**
 * The standalone ops console: one binary that serves the dashboard SPA as a pure gRPC client, the same
 * way in either deployment.
 *
 * <ul>
 *   <li>{@code WIGGLE_COORDINATOR_URL} set → coordinator mode: fan instance queries across
 *       {@code WIGGLE_NAMESPACE}'s active cells, route operate-by-id to the owning cell.</li>
 *   <li>otherwise → direct mode against {@code WIGGLE_URL} (default {@code localhost:8080}).</li>
 * </ul>
 *
 * Auth/TLS reuse the same {@code WIGGLE_DASHBOARD_*} / {@code WIGGLE_TLS_*} env as an embedded dashboard;
 * it serves on {@code WIGGLE_DASHBOARD_PORT} (default 8090).
 */
public final class ConsoleMain {

    public static void main(String[] args) throws Exception {
        Tls.Options tls = Tls.Options.fromEnvironment();
        String coordUrl = env("WIGGLE_COORDINATOR_URL", null);

        ConsoleBackend backend;
        AutoCloseable connection;
        String mode;
        if (coordUrl != null) {
            String namespace = env("WIGGLE_NAMESPACE", null);
            if (namespace == null) {
                throw new IllegalArgumentException("WIGGLE_NAMESPACE is required in coordinator mode");
            }
            CoordinatedConnection conn = WiggleConnection.coordinator(coordUrl, tls, env("WIGGLE_REGION", ""));
            backend = new ConsoleBackend.Coordinated(conn, namespace, tls);
            connection = conn;
            mode = "coordinator " + coordUrl + " namespace '" + namespace + "'";
        } else {
            String url = env("WIGGLE_URL", "localhost:8080");
            DirectConnection conn = WiggleConnection.direct(url, tls);
            backend = new ConsoleBackend.Direct(conn);
            connection = conn;
            mode = "direct " + url;
        }

        int port = Integer.parseInt(env("WIGGLE_DASHBOARD_PORT", "8090"));
        String user = env("WIGGLE_DASHBOARD_USER", "admin");
        String password = env("WIGGLE_DASHBOARD_PASSWORD", null);
        DashboardData data = new GrpcDashboardData(backend);
        HttpDashboard dashboard = new HttpDashboard(data, port, user, password, tls).start();

        System.out.println("wiggle console on " + (tls.hasKeyStore() ? "https" : "http") + "://localhost:" + port
                + "  ->  " + mode);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            dashboard.close();
            try { backend.close(); } catch (Exception ignored) { }
            try { connection.close(); } catch (Exception ignored) { }
        }));
        Thread.currentThread().join();
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? def : v;
    }

    private ConsoleMain() {}
}
