package com.wiggle.server;

import com.wiggle.server.cluster.ClusterManager;
import com.wiggle.server.cluster.Housekeeper;
import com.wiggle.server.cluster.QueueLagMonitor;
import com.wiggle.server.engine.DefinitionRegistry;
import com.wiggle.server.engine.InstanceIds;
import com.wiggle.server.engine.WorkflowEngine;
import com.wiggle.server.grpc.GrpcApi;
import com.wiggle.server.http.HealthServer;
import com.wiggle.server.store.Storage;
import com.wiggle.server.topology.Placement;

import java.io.IOException;

/**
 * The subsystems of a {@link WiggleServer}: everything a node runs <em>beyond</em> the shared
 * storage + {@link com.wiggle.server.cluster.ClusterManager} -- the engine, its housekeeping and the
 * control plane.
 */
final class ServerBundle {

    private final WorkflowEngine engine;
    private final Housekeeper housekeeper;
    private final QueueLagMonitor queueLagMonitor;
    private final GrpcApi api;
    /** A {@code /healthz} probe endpoint for Kubernetes, on the configured port; null if none. */
    private final HealthServer health;

    ServerBundle(ServerConfig config, Storage storage, ClusterManager cluster) throws IOException {
        super();
        this.engine = new WorkflowEngine(storage, new DefinitionRegistry(storage), config.defaultLease().toMillis(),
                config.topology() == null ? InstanceIds.across(storage.instanceShards())
                        : new Placement(config.topology(), System::currentTimeMillis));
        this.housekeeper = new Housekeeper(engine, cluster, config.pollInterval(),
                config.retention(), config.housekeepingBatch(), Housekeeper.adaptiveByDefault(),
                config.defaultLease());
        this.queueLagMonitor = new QueueLagMonitor(engine, cluster,
                config.queueLagCheckInterval(), config.queueLagWarnThreshold());
        this.api = new GrpcApi(engine, cluster, config.port(), config.maxLongPoll().toMillis(),
                config.tls(), config.memory());
        // The dashboard moved to the console; the former dashboard port now serves only /healthz.
        this.health = config.dashboardPort() <= 0 ? null : new HealthServer(config.dashboardPort());
    }

    public void start() {
        housekeeper.start();
        queueLagMonitor.start();
        api.start();
        if (health != null) health.start();
    }

    public void close() {
        if (health != null) health.close();
        api.close();
        queueLagMonitor.close();
        housekeeper.close();
    }

    public int port() { return api.port(); }


    public WorkflowEngine engine() { return engine; }
}
