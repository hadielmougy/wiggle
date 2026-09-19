package com.wiggle.server;

import com.wiggle.core.Ids;
import com.wiggle.placement.LivePlacement;
import com.wiggle.server.cluster.ClusterManager;
import com.wiggle.server.cluster.Housekeeper;
import com.wiggle.server.cluster.QueueLagMonitor;
import com.wiggle.server.engine.DefinitionRegistry;
import com.wiggle.server.engine.InstanceIds;
import com.wiggle.server.engine.WorkflowEngine;
import com.wiggle.server.grpc.GrpcApi;
import com.wiggle.server.http.HealthServer;
import com.wiggle.server.store.Storage;

import java.io.IOException;

/**
 * The cell subsystems of a {@link WiggleServer}. Everything a node runs <em>beyond</em> the shared
 * storage + {@link com.wiggle.server.cluster.ClusterManager} lives in a bundle ({@link ServerBundle}: the
 * engine + control plane). (The seam predates the coordinator's extraction into its own module; it is
 * kept for the placement/engine accessors.)
 */
final class ServerBundle {

    private final WorkflowEngine engine;
    private final Housekeeper housekeeper;
    private final QueueLagMonitor queueLagMonitor;
    private final GrpcApi api;
    /** A {@code /healthz} probe endpoint for Kubernetes, on the configured port; null if none. */
    private final HealthServer health;
    /** Null for a standalone cell; the coordinator-managed placement otherwise. */
    private final LivePlacement placement;

    ServerBundle(ServerConfig config, Storage storage, ClusterManager cluster) throws IOException {
        super();
        String ns = config.namespace();
        this.placement = ns == null || ns.isBlank() ? null : new LivePlacement();
        this.engine = new WorkflowEngine(storage, new DefinitionRegistry(storage), config.defaultLease().toMillis(),
                idMinter(ns, config.cellId(), placement()));
        this.housekeeper = new Housekeeper(engine, cluster, config.pollInterval(),
                config.retention(), config.housekeepingBatch());
        this.queueLagMonitor = new QueueLagMonitor(engine, cluster,
                config.queueLagCheckInterval(), config.queueLagWarnThreshold());
        this.api = new GrpcApi(engine, cluster, config.port(), config.maxLongPoll().toMillis(),
                config.tls(), config.memory());
        // The dashboard moved to the console; the former dashboard port now serves only /healthz.
        this.health = config.dashboardPort() <= 0 ? null : new HealthServer(config.dashboardPort());
    }

    /** The coordinator-managed placement (epoch + owned shards); null for a standalone cell. */
    public LivePlacement placement() { return placement; }

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

    /**
     * How new instance ids are minted: {@code ns[.c{cell}].e{epoch}.s{shard}.ulid}, or the legacy
     * {@code wfi_} form with no namespace. Epoch and shard come from the live {@link LivePlacement}
     * (0/0 until a coordinator registers one, and for a cell that has none).
     *
     * <p>The cell label is stamped whenever {@code WIGGLE_CELL_ID} is set, including on a cell with
     * no coordinator -- it is what still routes when there is no ring to consult.
     */
    private static InstanceIds idMinter(String ns, String cellId, LivePlacement placement) {
        if (ns == null || ns.isBlank()) {
            return () -> Ids.next("wfi");
        }
        LivePlacement live = placement == null ? new LivePlacement() : placement;
        return live.minter(ns, cellId, Ids::token)::get;   // :placement speaks Supplier; adapt here
    }
}
