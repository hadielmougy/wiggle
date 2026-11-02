package com.wiggle.server;

import com.wiggle.server.api.ApiServlet;
import com.wiggle.server.auth.Accounts;
import com.wiggle.server.auth.AuthCache;
import com.wiggle.server.cluster.ClusterManager;
import com.wiggle.server.engine.WorkflowEngine;
import com.wiggle.server.store.Storage;
import com.wiggle.server.store.StorageFactory;
import com.wiggle.server.topology.Topology;

import java.io.IOException;
import java.util.List;

/**
 * Wires one server node together. Multiple nodes pointed at the same JDBC URL form a
 * cluster: they all serve the API and hand out work, and exactly one of them holds the
 * leader role and runs the clock-driven housekeeping.
 *
 * <p>A {@code WiggleServer} is the engine + control plane, over shared storage and a
 * {@link ClusterManager} (membership + leader election).
 *
 * <p>The server core is storage-agnostic: the {@link StorageFactory} it is given builds the store for
 * the configured URL ({@code wiggle-postgres} supplies one covering every backend, including an H2
 * database in memory when no URL is set).
 */
public final class WiggleServer implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(WiggleServer.class.getName());

    /** How often a node reads the auth audit for changes made through any node. */
    private static final long AUTH_POLL_MILLIS = 1_000;

    private final ServerConfig config;
    private final Storage storage;
    private final ClusterManager cluster;
    private final ServerBundle bundle;
    private final Accounts accounts;
    private final AuthCache authCache;

    public WiggleServer(ServerConfig config, StorageFactory storageFactory) throws IOException {
        this(config, storageFactory, true);
    }

    /**
     * A node that opens no HTTP server of its own: a servlet container serves its API, through
     * {@link #apiServlet()}, and its probe.
     */
    public static WiggleServer withoutHttpServers(ServerConfig config, StorageFactory storageFactory) throws IOException {
        return new WiggleServer(config, storageFactory, false);
    }

    private WiggleServer(ServerConfig config, StorageFactory storageFactory, boolean servesHttp) throws IOException {
        this.config = config;
        this.storage = storageFactory.create(config);
        this.storage.migrate();
        this.accounts = new Accounts(storage, System::currentTimeMillis);
        this.accounts.bootstrap();
        this.authCache = new AuthCache(accounts, config.auth().cache().toMillis(), System::currentTimeMillis);
        Topology topology = config.topology();
        this.cluster = new ClusterManager(storage, config.nodeName(), Runtime.getRuntime().availableProcessors(),
                config.heartbeatInterval().toMillis(), config.missedHeartbeatsBeforeDead(),
                topology == null ? 0 : topology.newestGeneration().id(),
                topology == null ? () -> 0 : () -> topology.generationAt(System.currentTimeMillis()).id());
        this.bundle = new ServerBundle(config, storage, cluster, authCache, servesHttp);
    }

    public WiggleServer start() {
        if (config.auth().api() != ServerConfig.ApiAuth.OFF) authCache.start(AUTH_POLL_MILLIS);
        cluster.start();
        bundle.start();
        LOG.log(System.Logger.Level.INFO, () -> "server node '" + config.nodeName()
                + "' started" + (port() < 0 ? "" : " on port " + port())
                + " (storage: " + (config.isInMemory() ? "in-memory" : "jdbc") + ")");
        return this;
    }

    /** The port the node's own API server listens on; -1 for a node {@link #withoutHttpServers}. */
    public int port() { return bundle.port(); }

    /** The API as a servlet, for {@code ControlPlane.PREFIX + "*"}. */
    public ApiServlet apiServlet() { return bundle.apiServlet(); }

    /** This node's API address on the loopback interface, for in-process clients and local runs. */
    public String baseUrl() { return "127.0.0.1:" + port(); }

    /** Full-text search over instances, when it is enabled. */
    public java.util.Optional<com.wiggle.server.search.Search> search() {
        return java.util.Optional.ofNullable(bundle.search());
    }

    /** The workflow engine. */
    public WorkflowEngine engine() { return bundle.engine(); }

    public ClusterManager cluster() { return cluster; }

    /** Portal accounts, roles and sessions on the auth shard. Not reachable over the API. */
    public Accounts accounts() { return accounts; }

    /** This node's cache of accounts, sessions and credentials, polling the audit once started. */
    public AuthCache authCache() { return authCache.start(AUTH_POLL_MILLIS); }

    @Override public void close() {
        LOG.log(System.Logger.Level.INFO, () -> "node '" + config.nodeName() + "' stopping");
        bundle.close();
        authCache.close();
        cluster.close();
        storage.close();
    }
}
