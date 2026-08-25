package dev.wiggle.server;

import dev.wiggle.server.cluster.ClusterManager;
import dev.wiggle.server.cluster.Housekeeper;
import dev.wiggle.server.cluster.QueueLagMonitor;
import dev.wiggle.server.engine.DefinitionRegistry;
import dev.wiggle.server.engine.WorkflowEngine;
import dev.wiggle.server.grpc.GrpcApi;
import dev.wiggle.server.http.DashboardAuth;
import dev.wiggle.server.http.HttpDashboard;
import dev.wiggle.server.store.InMemoryStorage;
import dev.wiggle.server.store.Storage;
import dev.wiggle.server.store.StorageFactory;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;

/**
 * Wires one server node together. Multiple nodes pointed at the same JDBC URL form a
 * cluster: they all serve the API and hand out work, and exactly one of them holds the
 * leader role and runs the clock-driven housekeeping.
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
    private final WorkflowEngine engine;
    private final ClusterManager cluster;
    private final Housekeeper housekeeper;
    private final QueueLagMonitor queueLagMonitor;
    private final GrpcApi api;
    /** Null unless a dashboard port was configured. */
    private final HttpDashboard dashboard;

    /** In-memory only. To run on a database, use {@link #WiggleServer(ServerConfig, StorageFactory)}. */
    public WiggleServer(ServerConfig config) throws IOException {
        this(config, WiggleServer::inMemoryOnly);
    }

    public WiggleServer(ServerConfig config, StorageFactory storageFactory) throws IOException {
        this(config, storageFactory, null);
    }

    /**
     * @param dashboardAuth a custom dashboard authenticator (e.g. SSO), or {@code null} to use the
     *                      built-in admin-password login from {@link ServerConfig}.
     */
    public WiggleServer(ServerConfig config, StorageFactory storageFactory,
                        DashboardAuth dashboardAuth) throws IOException {
        this.config = config;
        this.storage = storageFactory.create(config);
        this.storage.migrate();
        this.engine = new WorkflowEngine(storage, new DefinitionRegistry(storage), config.defaultLease().toMillis());
        this.cluster = new ClusterManager(storage, config.nodeName(), Runtime.getRuntime().availableProcessors(),
                config.heartbeatInterval().toMillis(), config.missedHeartbeatsBeforeDead());
        this.housekeeper = new Housekeeper(engine, cluster, config.pollInterval(),
                config.retention(), config.housekeepingBatch());
        this.queueLagMonitor = new QueueLagMonitor(engine, cluster,
                config.queueLagCheckInterval(), config.queueLagWarnThreshold());
        this.api = new GrpcApi(engine, cluster, config.port(), config.maxLongPoll().toMillis(),
                config.tls(), config.memory());
        this.dashboard = config.dashboardPort() <= 0 ? null : getHttpDashboard(config, dashboardAuth);
    }

    private @NonNull HttpDashboard getHttpDashboard(ServerConfig config, DashboardAuth dashboardAuth) throws IOException {
        return dashboardAuth != null
                ? new HttpDashboard(engine, cluster, config.dashboardPort(), dashboardAuth, config.tls())
                : new HttpDashboard(engine, cluster, config.dashboardPort(), config.dashboardUser(), config.dashboardPassword(), config.tls());
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
        housekeeper.start();
        queueLagMonitor.start();
        api.start();
        if (dashboard != null) dashboard.start();
        LOG.log(System.Logger.Level.INFO, () -> "node '" + config.nodeName() + "' started on port " + port()
                + " (storage: " + (config.isInMemory() ? "in-memory" : "jdbc")
                + (dashboard != null ? ", dashboard: " + dashboard.port() : "") + ")");
        return this;
    }

    public int port() { return api.port(); }

    /** The dashboard's port, or {@code -1} if it is not enabled. */
    public int dashboardPort() { return dashboard == null ? -1 : dashboard.port(); }

    public String baseUrl() { return "127.0.0.1:" + port(); }

    public WorkflowEngine engine() { return engine; }

    public ClusterManager cluster() { return cluster; }

    @Override public void close() {
        LOG.log(System.Logger.Level.INFO, () -> "node '" + config.nodeName() + "' stopping");
        if (dashboard != null) dashboard.close();
        api.close();
        queueLagMonitor.close();
        housekeeper.close();
        cluster.close();
        storage.close();
    }
}
