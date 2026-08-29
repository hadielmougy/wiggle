package dev.wiggle.server;

import dev.wiggle.server.cluster.ClusterManager;
import dev.wiggle.server.engine.WorkflowEngine;
import dev.wiggle.server.http.DashboardAuth;
import dev.wiggle.server.store.InMemoryStorage;
import dev.wiggle.server.store.Storage;
import dev.wiggle.server.store.StorageFactory;

import java.io.IOException;

/**
 * Wires one server node together. Multiple nodes pointed at the same JDBC URL form a
 * cluster: they all serve the API and hand out work, and exactly one of them holds the
 * leader role and runs the clock-driven housekeeping.
 *
 * <p>A node runs in a {@link ServerRole}. The shared pieces -- storage and the
 * {@link ClusterManager} (membership + leader election) -- are the same for every role; the
 * role-specific subsystems live in a {@link ServerBundle} (a {@link CellBundle} for the engine +
 * control plane, a {@link CoordinatorBundle} for the coordinator surface). The default role is
 * {@code cell}, so a server started without {@code WIGGLE_ROLE} behaves exactly as it always has.
 *
 * <p>The server core is storage-agnostic. With no URL configured it uses the in-memory store; to
 * run on a database, pass a {@link StorageFactory} that knows how to build the store for the URL
 * (the standalone {@code wiggle-dist} distribution supplies one covering every backend). The
 * single-argument constructor is in-memory only, so an embedder that wants a database must use the
 * two-argument form.
 */
public final class WiggleServer implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(WiggleServer.class.getName());

    private final ServerConfig config;
    private final Storage storage;
    private final ClusterManager cluster;
    private final ServerBundle bundle;

    /** In-memory only. To run on a database, use {@link #WiggleServer(ServerConfig, StorageFactory)}. */
    public WiggleServer(ServerConfig config) throws IOException {
        this(config, WiggleServer::inMemoryOnly);
    }

    public WiggleServer(ServerConfig config, StorageFactory storageFactory) throws IOException {
        this(config, storageFactory, null);
    }

    /**
     * @param dashboardAuth a custom dashboard authenticator (e.g. SSO), or {@code null} to use the
     *                      built-in admin-password login from {@link ServerConfig}. Ignored by the
     *                      coordinator role, which serves no dashboard.
     */
    public WiggleServer(ServerConfig config, StorageFactory storageFactory,
                        DashboardAuth dashboardAuth) throws IOException {
        this.config = config;
        this.storage = storageFactory.create(config);
        this.storage.migrate(config.role());
        this.cluster = new ClusterManager(storage, config.nodeName(), Runtime.getRuntime().availableProcessors(),
                config.heartbeatInterval().toMillis(), config.missedHeartbeatsBeforeDead());
        this.bundle = switch (config.role()) {
            case COORDINATOR -> new CoordinatorBundle(config, storage, cluster);
            case CELL -> new CellBundle(config, storage, cluster, dashboardAuth);
        };
    }

    /** The default factory: in-memory when no URL is set, otherwise a clear error pointing at the two-arg form. */
    private static Storage inMemoryOnly(ServerConfig config) {
        if (config.isInMemory()) return new InMemoryStorage();
        throw new IllegalStateException("a storage URL is set ('" + config.jdbcUrl()
                + "') but no StorageFactory was provided -- use WiggleServer(config, factory), or run the "
                + "standalone distribution (wiggle-dist), which wires every backend by URL scheme");
    }

    public WiggleServer start() {
        cluster.start();
        bundle.start();
        LOG.log(System.Logger.Level.INFO, () -> "node '" + config.nodeName() + "' (" + config.role()
                + ") started on port " + port()
                + " (storage: " + (config.isInMemory() ? "in-memory" : "jdbc")
                + (dashboardPort() > 0 ? ", dashboard: " + dashboardPort() : "") + ")");
        return this;
    }

    public int port() { return bundle.port(); }

    /** The dashboard's port, or {@code -1} if it is not enabled. */
    public int dashboardPort() { return bundle.dashboardPort(); }

    public String baseUrl() { return "127.0.0.1:" + port(); }

    /** The workflow engine. Valid for the {@code cell} role; throws for {@code coordinator}. */
    public WorkflowEngine engine() { return bundle.engine(); }

    /**
     * The coordinator-managed placement (mint epoch + owned shards), or {@code null} for a standalone
     * cell (no namespace) and for the coordinator role. The coordinator link updates it as policy moves.
     */
    public CellPlacement placement() { return bundle.placement(); }

    public ClusterManager cluster() { return cluster; }

    @Override public void close() {
        LOG.log(System.Logger.Level.INFO, () -> "node '" + config.nodeName() + "' stopping");
        bundle.close();
        cluster.close();
        storage.close();
    }
}
