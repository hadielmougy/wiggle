package dev.wiggle.server;

import dev.wiggle.core.IdCodec;
import dev.wiggle.core.Ids;
import dev.wiggle.server.cluster.ClusterManager;
import dev.wiggle.server.cluster.Housekeeper;
import dev.wiggle.server.cluster.QueueLagMonitor;
import dev.wiggle.server.engine.DefinitionRegistry;
import dev.wiggle.server.engine.WorkflowEngine;
import dev.wiggle.server.grpc.GrpcApi;
import dev.wiggle.server.http.DashboardAuth;
import dev.wiggle.server.http.HttpDashboard;
import dev.wiggle.server.store.Storage;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * The {@link ServerRole#CELL} subsystems: the workflow engine, the clock-driven housekeeping, the
 * queue-lag monitor, the {@code WiggleControlPlane} gRPC API, and (optionally) the HTTP dashboard.
 * This is exactly the wiring {@link WiggleServer} used to hold inline; extracting it lets a
 * coordinator node skip all of it.
 */
final class CellBundle implements ServerBundle {

    private final WorkflowEngine engine;
    private final Housekeeper housekeeper;
    private final QueueLagMonitor queueLagMonitor;
    private final GrpcApi api;
    /** Null unless a dashboard port was configured. */
    private final HttpDashboard dashboard;

    CellBundle(ServerConfig config, Storage storage, ClusterManager cluster,
               DashboardAuth dashboardAuth) throws IOException {
        this.engine = new WorkflowEngine(storage, new DefinitionRegistry(storage), config.defaultLease().toMillis(),
                idMinter(config));
        this.housekeeper = new Housekeeper(engine, cluster, config.pollInterval(),
                config.retention(), config.housekeepingBatch());
        this.queueLagMonitor = new QueueLagMonitor(engine, cluster,
                config.queueLagCheckInterval(), config.queueLagWarnThreshold());
        this.api = new GrpcApi(engine, cluster, config.port(), config.maxLongPoll().toMillis(),
                config.tls(), config.memory());
        this.dashboard = config.dashboardPort() <= 0 ? null : dashboard(config, dashboardAuth, engine, cluster);
    }

    /**
     * How new instance ids are minted. With a namespace configured (a coordinator-managed cell) the
     * id is epoch-aware ({@code ns.e0.s0.ulid}); the epoch/ring are 0/1 until the coordinator supplies
     * a real ring (a later ticket). Without a namespace (standalone) it stays the legacy {@code wfi_}
     * form, which the codec treats as a legacy id routed to the genesis cell.
     */
    private static Supplier<String> idMinter(ServerConfig config) {
        String ns = config.namespace();
        if (ns == null || ns.isBlank()) {
            return () -> Ids.next("wfi");
        }
        return () -> {
            String ulid = Ids.token();
            return IdCodec.format(ns, 0, IdCodec.shardFor(ulid, 1), ulid);
        };
    }

    private static @NonNull HttpDashboard dashboard(ServerConfig config, DashboardAuth dashboardAuth,
                                                    WorkflowEngine engine, ClusterManager cluster) throws IOException {
        return dashboardAuth != null
                ? new HttpDashboard(engine, cluster, config.dashboardPort(), dashboardAuth, config.tls())
                : new HttpDashboard(engine, cluster, config.dashboardPort(),
                        config.dashboardUser(), config.dashboardPassword(), config.tls());
    }

    @Override public void start() {
        housekeeper.start();
        queueLagMonitor.start();
        api.start();
        if (dashboard != null) dashboard.start();
    }

    @Override public void close() {
        if (dashboard != null) dashboard.close();
        api.close();
        queueLagMonitor.close();
        housekeeper.close();
    }

    @Override public int port() { return api.port(); }

    @Override public int dashboardPort() { return dashboard == null ? -1 : dashboard.port(); }

    @Override public WorkflowEngine engine() { return engine; }
}
