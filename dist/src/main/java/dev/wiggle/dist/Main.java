package dev.wiggle.dist;

import dev.wiggle.dist.coord.ConfigSource;
import dev.wiggle.dist.coord.CoordinatorConfigSource;
import dev.wiggle.dist.coord.CoordinatorLink;
import dev.wiggle.dist.coord.EnvConfigSource;
import dev.wiggle.dist.coord.HttpCoordinatorLink;
import dev.wiggle.dist.coord.NoopCoordinatorLink;
import dev.wiggle.server.Logging;
import dev.wiggle.server.ServerConfig;
import dev.wiggle.server.WiggleServer;

/**
 * Entry point for the standalone server distribution. Reads configuration from the environment,
 * wires the all-backends {@link WiggleStorageFactory}, and runs until the JVM is stopped.
 *
 * <p>The cell coordinator is optional and off by default: with no {@code WIGGLE_COORDINATOR_URL} the
 * node uses env config and a no-op coordinator link -- behaviour identical to a standalone server.
 * When the URL is set, config comes through a {@link CoordinatorConfigSource} and the node announces
 * itself via a {@link CoordinatorLink} (both best-effort; a coordinator outage never blocks boot).
 */
public final class Main {

    private Main() { }

    public static void main(String[] args) throws Exception {
        Logging.configureFromEnv();   // opt-in file logging, before anything logs

        String coordinatorUrl = System.getenv("WIGGLE_COORDINATOR_URL");
        boolean coordinated = coordinatorUrl != null && !coordinatorUrl.isBlank();
        ConfigSource configSource = coordinated
                ? new CoordinatorConfigSource(new EnvConfigSource(), coordinatorUrl)
                : new EnvConfigSource();
        CoordinatorLink coordinator = coordinated
                ? new HttpCoordinatorLink(coordinatorUrl)
                : new NoopCoordinatorLink();

        ServerConfig config = configSource.load();
        WiggleServer server = new WiggleServer(config, new WiggleStorageFactory()).start();
        boolean tls = config.tls().hasKeyStore();
        System.out.println("Wiggle server '" + config.nodeName() + "' on " + server.baseUrl()
                + " (gRPC: " + (tls ? "TLS" : "plaintext")
                + ", storage: " + (config.isInMemory() ? "in-memory" : config.jdbcUrl()) + ")");
        String logFile = System.getenv("WIGGLE_LOG_FILE");
        if (logFile != null && !logFile.isBlank()) System.out.println("Logging to " + logFile);
        if (server.dashboardPort() > 0) {
            System.out.println("Dashboard at " + (tls ? "https" : "http") + "://localhost:" + server.dashboardPort());
        }

        String cellId = System.getenv().getOrDefault("WIGGLE_CELL_ID", "");
        // A coordinator-role node runs no cell (no placement, no engine) -> no runtime to report.
        CoordinatorLink.CellRuntime runtime = server.placement() == null ? null
                : new CoordinatorLink.CellRuntime(server.placement(), server.engine()::liveCountByEpoch);
        coordinator.register(new CoordinatorLink.NodeInfo(
                config.nodeName(), config.namespace(), cellId, server.baseUrl(), engineVersion()), runtime);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                coordinator.close();
            } finally {
                server.close();
            }
        }));
        Thread.currentThread().join();
    }

    private static String engineVersion() {
        String v = WiggleServer.class.getPackage().getImplementationVersion();
        return v != null ? v : "dev";
    }
}
