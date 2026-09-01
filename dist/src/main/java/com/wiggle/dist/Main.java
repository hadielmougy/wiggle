package com.wiggle.dist;

import com.wiggle.dist.coord.ConfigSource;
import com.wiggle.dist.coord.CoordinatorConfigSource;
import com.wiggle.dist.coord.CoordinatorLink;
import com.wiggle.dist.coord.EnvConfigSource;
import com.wiggle.dist.coord.HttpCoordinatorLink;
import com.wiggle.dist.coord.NoopCoordinatorLink;
import com.wiggle.server.Logging;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import com.wiggle.coordinator.ratis.RatisCoordinatorStoreProvider;
import com.wiggle.server.coord.CoordinatorServer;
import com.wiggle.server.coord.CoordinatorStore;

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

        ServerConfig config = configSource.load();

        // Cell vs coordinator is an app-layer choice (WIGGLE_ROLE), not an engine concept: the engine
        // and the coordinator are decoupled libraries composed here.
        if ("coordinator".equalsIgnoreCase(System.getenv().getOrDefault("WIGGLE_ROLE", "cell").trim())) {
            runCoordinator(config);   // a separate, engine-free control plane; never a WiggleServer
            return;
        }

        CoordinatorLink coordinator = coordinated
                ? new HttpCoordinatorLink(coordinatorUrl)
                : new NoopCoordinatorLink();

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
                config.nodeName(), config.namespace(), cellId, server.baseUrl(), engineVersion(),
                server.cellFingerprint()), runtime);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                coordinator.close();
            } finally {
                server.close();
            }
        }));
        Thread.currentThread().join();
    }

    /**
     * Runs the coordinator control plane: build its store, then a self-hosted {@link CoordinatorServer}.
     * No engine, no cell. The store is the embedded Ratis + RocksDB backend — a consensus-backed control
     * plane with no external store and no engine database. Its data directory (and, for a multi-node group,
     * its peers) come from {@code WIGGLE_COORD_STORE=ratis://<dir>?peers=...}, defaulting to a single-member
     * group at {@code /var/lib/wiggle/coord}. Leader election is a durable lease over that store, keyed on
     * the node name.
     */
    private static void runCoordinator(ServerConfig config) throws Exception {
        String uri = System.getenv("WIGGLE_COORD_STORE");
        if (uri == null || uri.isBlank()) uri = "ratis:///var/lib/wiggle/coord";
        if (!uri.startsWith("ratis:")) {
            throw new IllegalArgumentException("the coordinator store is Ratis-only; set "
                    + "WIGGLE_COORD_STORE=ratis://<dir>?peers=... (got '" + uri + "')");
        }
        CoordinatorStore store = new RatisCoordinatorStoreProvider(uri).coordinatorStore();
        CoordinatorServer coordinator = new CoordinatorServer(store, config.port(), config.tls(),
                config.missedHeartbeatsBeforeDead(), config.nodeName()).start();
        boolean tls = config.tls().hasKeyStore();
        System.out.println("Wiggle coordinator '" + config.nodeName() + "' on 127.0.0.1:" + coordinator.port()
                + " (gRPC: " + (tls ? "TLS" : "plaintext") + ", store: ratis " + uri + ")");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            coordinator.close();
            try { store.close(); } catch (Exception ignored) { }
        }));
        Thread.currentThread().join();
    }

    private static String engineVersion() {
        String v = WiggleServer.class.getPackage().getImplementationVersion();
        return v != null ? v : "dev";
    }
}
