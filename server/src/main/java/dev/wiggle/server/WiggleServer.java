package dev.wiggle.server;

import dev.wiggle.server.cluster.ClusterManager;
import dev.wiggle.server.cluster.Housekeeper;
import dev.wiggle.server.engine.DefinitionRegistry;
import dev.wiggle.server.engine.WorkflowEngine;
import dev.wiggle.server.grpc.GrpcApi;
import dev.wiggle.server.store.InMemoryStorage;
import dev.wiggle.server.store.JdbcStorage;
import dev.wiggle.server.store.Storage;

import java.io.IOException;

/**
 * Wires one server node together. Multiple nodes pointed at the same JDBC URL form a
 * cluster: they all serve the API and hand out work, and exactly one of them holds the
 * leader role and runs the clock-driven housekeeping.
 */
public final class WiggleServer implements AutoCloseable {

    private final ServerConfig config;
    private final Storage storage;
    private final WorkflowEngine engine;
    private final ClusterManager cluster;
    private final Housekeeper housekeeper;
    private final GrpcApi api;

    public WiggleServer(ServerConfig config) throws IOException {
        this.config = config;
        this.storage = config.isInMemory()
                ? new InMemoryStorage()
                : new JdbcStorage(config.jdbcUrl(), config.jdbcUser(), config.jdbcPassword(), config.jdbcPoolSize());
        this.storage.migrate();
        this.engine = new WorkflowEngine(storage, new DefinitionRegistry(storage), config.defaultLease().toMillis());
        this.cluster = new ClusterManager(storage, config.nodeName(), Runtime.getRuntime().availableProcessors(),
                config.heartbeatInterval().toMillis(), config.missedHeartbeatsBeforeDead());
        this.housekeeper = new Housekeeper(engine, cluster, config.pollInterval(),
                config.retention(), config.housekeepingBatch());
        this.api = new GrpcApi(engine, cluster, config.port(), config.maxLongPoll().toMillis());
    }

    public WiggleServer start() {
        // Bind the API first so the real port is known (it may be ephemeral, port 0),
        // then advertise a dialable address before the cluster's first heartbeat writes it.
        api.start();
        cluster.setAdvertisedAddress(advertisedAddress());
        cluster.start();
        housekeeper.start();
        return this;
    }

    public int port() { return api.port(); }

    public String baseUrl() { return "127.0.0.1:" + port(); }

    private String advertisedAddress() {
        String configured = config.advertisedAddress();
        return configured != null && !configured.isBlank() ? configured : baseUrl();
    }

    public WorkflowEngine engine() { return engine; }

    public ClusterManager cluster() { return cluster; }

    @Override public void close() {
        api.close();
        housekeeper.close();
        cluster.close();
        storage.close();
    }

    public static void main(String[] args) throws Exception {
        ServerConfig config = ServerConfig.fromEnvironment();
        WiggleServer server = new WiggleServer(config).start();
        System.out.println("Wiggle server '" + config.nodeName() + "' on " + server.baseUrl()
                + " (storage: " + (config.isInMemory() ? "in-memory" : config.jdbcUrl()) + ")");
        Runtime.getRuntime().addShutdownHook(new Thread(server::close));
        Thread.currentThread().join();
    }
}
