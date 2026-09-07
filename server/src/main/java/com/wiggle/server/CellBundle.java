package com.wiggle.server;

import com.wiggle.core.IdCodec;
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
                idMinter(ns, placement));
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
     * How new instance ids are minted. With a namespace configured (a coordinator-managed cell) the id
     * is epoch-aware ({@code ns.e{epoch}.s{shard}.ulid}), with the epoch and shard taken from the live
     * {@link CellPlacement} the coordinator supplies at registration (defaulting to epoch 0 / shard 0
     * until then). Without a namespace (standalone) it stays the legacy {@code wfi_} form, which the
     * codec treats as a legacy id routed to the genesis cell.
     */
    private static Supplier<String> idMinter(String ns, CellPlacement placement) {
        if (placement == null) {
            return () -> Ids.next("wfi");
        }
        return () -> {
            String ulid = Ids.token();
            CellPlacement.Stamp st = placement.stampFor(ulid);   // atomic (epoch, shard) -- see CellPlacement
            return IdCodec.format(ns, st.epoch(), st.shard(), ulid);
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
