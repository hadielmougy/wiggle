package com.wiggle.server;

import com.wiggle.placement.IdCodec;
import com.wiggle.core.Ids;
import com.wiggle.server.cluster.ClusterManager;
import com.wiggle.server.cluster.Housekeeper;
import com.wiggle.server.cluster.QueueLagMonitor;
import com.wiggle.server.engine.DefinitionRegistry;
import com.wiggle.server.engine.WorkflowEngine;
import com.wiggle.server.grpc.GrpcApi;
import com.wiggle.server.http.HealthServer;
import com.wiggle.server.store.Storage;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * The cell subsystems: the workflow engine, the clock-driven housekeeping, the
 * queue-lag monitor, and the {@code WiggleControlPlane} gRPC API. This is exactly the wiring
 * {@link WiggleServer} used to hold inline; extracting it lets a coordinator node skip all of it. The
 * ops UI is a separate process (the {@code console} module), a pure gRPC client -- cells serve no web UI.
 */
final class CellBundle implements ServerBundle {

    private final WorkflowEngine engine;
    private final Housekeeper housekeeper;
    private final QueueLagMonitor queueLagMonitor;
    private final GrpcApi api;
    /** A {@code /healthz} probe endpoint for Kubernetes, on the configured port; null if none. */
    private final HealthServer health;
    /** Null for a standalone cell; the coordinator-managed placement otherwise. */
    private final CellPlacement placement;

    CellBundle(ServerConfig config, Storage storage, ClusterManager cluster) throws IOException {
        String ns = config.namespace();
        this.placement = ns == null || ns.isBlank() ? null : new CellPlacement();
        this.engine = new WorkflowEngine(storage, new DefinitionRegistry(storage), config.defaultLease().toMillis(),
                idMinter(ns, config.cellId(), placement));
        this.housekeeper = new Housekeeper(engine, cluster, config.pollInterval(),
                config.retention(), config.housekeepingBatch());
        this.queueLagMonitor = new QueueLagMonitor(engine, cluster,
                config.queueLagCheckInterval(), config.queueLagWarnThreshold());
        this.api = new GrpcApi(engine, cluster, config.port(), config.maxLongPoll().toMillis(),
                config.tls(), config.memory());
        // The dashboard moved to the console; the former dashboard port now serves only /healthz.
        this.health = config.dashboardPort() <= 0 ? null : new HealthServer(config.dashboardPort());
    }

    /**
     * How new instance ids are minted: {@code ns[.c{cell}].e{epoch}.s{shard}.ulid}.
     *
     * <p>The namespace is what makes an id routable at all, so without one this stays the legacy
     * {@code wfi_} form. Epoch and shard come from the live {@link CellPlacement} the coordinator
     * supplies at registration (epoch 0 / shard 0 until then, and for a cell with no coordinator).
     *
     * <p>The cell label is stamped whenever {@code WIGGLE_CELL_ID} is set, <em>including</em> on a
     * cell that runs without a coordinator. That is the point of it: epoch and shard say where an
     * instance belongs under a ring, the cell says where it was actually written, and only the
     * second one still means something in a deployment that has no ring to consult.
     */
    private static Supplier<String> idMinter(String ns, String cellId, CellPlacement placement) {
        if (ns == null || ns.isBlank()) {
            return () -> Ids.next("wfi");
        }
        CellPlacement live = placement == null ? new CellPlacement() : placement;
        return () -> {
            String ulid = Ids.token();
            CellPlacement.Stamp st = live.stampFor(ulid);   // atomic (epoch, shard) -- see CellPlacement
            return IdCodec.format(ns, cellId, st.epoch(), st.shard(), ulid);
        };
    }

    /** The coordinator-managed placement (epoch + owned shards); null for a standalone cell. */
    @Override public CellPlacement placement() { return placement; }

    @Override public void start() {
        housekeeper.start();
        queueLagMonitor.start();
        api.start();
        if (health != null) health.start();
    }

    @Override public void close() {
        if (health != null) health.close();
        api.close();
        queueLagMonitor.close();
        housekeeper.close();
    }

    @Override public int port() { return api.port(); }

    @Override public WorkflowEngine engine() { return engine; }
}
