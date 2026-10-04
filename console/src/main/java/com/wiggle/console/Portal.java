package com.wiggle.console;

import com.wiggle.core.Tls;
import com.wiggle.server.WiggleServer;
import com.wiggle.server.auth.Accounts;
import com.wiggle.server.auth.AuthCache;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * The portal: the dashboard SPA and its JSON API, served by a server process over its own engine
 * on {@code WIGGLE_PORTAL_PORT}, separate from the gRPC port.
 *
 * <p>Built-in accounts come from {@code WIGGLE_DASHBOARD_USER} / {@code WIGGLE_DASHBOARD_PASSWORD} and
 * the optional read-only {@code WIGGLE_DASHBOARD_VIEWER_*} pair. Every other account, role and
 * session is on the auth shard, read through a per-node cache whose entries live at most
 * {@code WIGGLE_AUTH_CACHE_MILLIS} (default 30 s). A users file at {@code WIGGLE_CONSOLE_USERS_FILE}
 * from before is imported once and then no longer read.
 */
public final class Portal implements AutoCloseable {

    public static final String PORT_ENV = "WIGGLE_PORTAL_PORT";
    private static final System.Logger LOG = System.getLogger(Portal.class.getName());
    private static final long DEFAULT_CACHE_MILLIS = 30_000;
    private static final long POLL_MILLIS = 1_000;

    private final ConsoleServer http;
    private final AuthCache cache;

    private Portal(ConsoleServer http, AuthCache cache) {
        this.http = http;
        this.cache = cache;
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
        long cacheMillis = Long.parseLong(get(env, "WIGGLE_AUTH_CACHE_MILLIS", String.valueOf(DEFAULT_CACHE_MILLIS)));
        Accounts accounts = server.accounts();
        AuthCache cache = new AuthCache(accounts, cacheMillis, System::currentTimeMillis).start(POLL_MILLIS);
        try {
            ConsoleAuth auth = new ConsoleAuth(get(env, "WIGGLE_DASHBOARD_USER", "admin"),
                    get(env, "WIGGLE_DASHBOARD_PASSWORD", null),
                    get(env, "WIGGLE_DASHBOARD_VIEWER_USER", "viewer"),
                    get(env, "WIGGLE_DASHBOARD_VIEWER_PASSWORD", null),
                    tls.hasKeyStore(), accounts, cache);
            importUsersFile(accounts, Path.of(get(env, "WIGGLE_CONSOLE_USERS_FILE", "wiggle-users.json")),
                    auth.builtinNames());
            DashboardData data = new EngineDashboardData(server.engine(), server.cluster());
            return Optional.of(new Portal(new ConsoleServer(data, auth, port, tls).start(), cache));
        } catch (RuntimeException e) {
            cache.close();
            throw e;
        }
    }

    /** Imports the users file of a console from before the auth shard, the first time any node sees one. */
    static void importUsersFile(Accounts accounts, Path file, Set<String> reserved) {
        if (!Files.exists(file)) return;
        int imported = accounts.importFile(file, reserved);
        if (imported > 0) {
            LOG.log(System.Logger.Level.INFO, () -> "imported " + imported + " account(s) from " + file
                    + " to the auth shard");
        }
        LOG.log(System.Logger.Level.WARNING, () -> "accounts live on the auth shard; " + file
                + " is no longer read and can be removed");
    }

    /** The port it is listening on. */
    public int port() {
        return http.port();
    }

    @Override public void close() {
        http.close();
        cache.close();
    }

    private static String get(Map<String, String> env, String key, String def) {
        String v = env.get(key);
        return v == null || v.isBlank() ? def : v;
    }
}
