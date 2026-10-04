package com.wiggle.server;

import com.wiggle.server.auth.Accounts;
import com.wiggle.server.auth.AuthCache;
import com.wiggle.server.cluster.ClusterManager;
import com.wiggle.server.engine.WorkflowEngine;
import com.wiggle.server.store.InMemoryStorage;
import com.wiggle.server.store.Storage;
import com.wiggle.server.store.StorageFactory;
import com.wiggle.server.topology.Topology;

import java.io.IOException;

/**
 * Wires one server node together. Multiple nodes pointed at the same JDBC URL form a
 * cluster: they all serve the API and hand out work, and exactly one of them holds the
 * leader role and runs the clock-driven housekeeping.
 *
 * <p>A {@code WiggleServer} is the engine + control plane, over shared storage and a
 * {@link ClusterManager} (membership + leader election).
 *
 * <p>The server core is storage-agnostic. With no URL configured it uses the in-memory store; to
 * run on a database, pass a {@link StorageFactory} that knows how to build the store for the URL
 * (the standalone {@code wiggle-dist} distribution supplies one covering every backend). The
 * single-argument constructor is in-memory only, so an embedder that wants a database must use the
 * two-argument form.
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

    /** In-memory only. To run on a database, use {@link #WiggleServer(ServerConfig, StorageFactory)}. */
    public WiggleServer(ServerConfig config) throws IOException {
        this(config, WiggleServer::inMemoryOnly);
    }

    public WiggleServer(ServerConfig config, StorageFactory storageFactory) throws IOException {
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
        this.bundle = new ServerBundle(config, storage, cluster, authCache);
    }

    /** The default factory: in-memory when no URL is set, otherwise a clear error pointing at the two-arg form. */
    private static Storage inMemoryOnly(ServerConfig config) {
        if (config.isInMemory()) return new InMemoryStorage();
        throw new IllegalStateException("a storage URL is set ('" + config.jdbcUrl()
                + "') but no StorageFactory was provided -- use WiggleServer(config, factory), or run the "
                + "standalone distribution (wiggle-dist), which wires every backend by URL scheme");
    }

    public WiggleServer start() {
        if (config.auth().grpc() != ServerConfig.GrpcAuth.OFF) authCache.start(AUTH_POLL_MILLIS);
        cluster.start();
        bundle.start();
        LOG.log(System.Logger.Level.INFO, () -> "server node '" + config.nodeName()
                + "' started on port " + port()
                + " (storage: " + (config.isInMemory() ? "in-memory" : "jdbc") + ")");
        return this;
    }

    public int port() { return bundle.port(); }

    /** This node's gRPC address on the loopback interface, for in-process clients and local runs. */
    public String baseUrl() { return "127.0.0.1:" + port(); }

    /** The workflow engine. */
    public WorkflowEngine engine() { return bundle.engine(); }

    public ClusterManager cluster() { return cluster; }

    /** Portal accounts, roles and sessions on the auth shard. Not reachable over gRPC. */
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
