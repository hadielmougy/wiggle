package dev.wiggle.server;

import dev.wiggle.server.cluster.ClusterManager;
import dev.wiggle.server.coord.CoordinatorApi;
import dev.wiggle.server.coord.CoordinatorReconciler;
import dev.wiggle.server.coord.CoordinatorStore;
import dev.wiggle.server.engine.WorkflowEngine;
import dev.wiggle.server.store.Storage;

import java.io.IOException;

/**
 * The {@link ServerRole#COORDINATOR} subsystems: the {@code CellCoordinator} gRPC service and a
 * leader-only reconcile loop, over the shared storage's {@link CoordinatorStore} and the shared
 * {@link ClusterManager} (election). A coordinator runs no engine, no dispatch, no
 * {@code WiggleControlPlane}.
 */
final class CoordinatorBundle implements ServerBundle {

    private static final System.Logger LOG = System.getLogger(CoordinatorBundle.class.getName());

    private final CoordinatorStore store;
    private final CoordinatorApi api;
    private final CoordinatorReconciler reconciler;

    CoordinatorBundle(ServerConfig config, Storage storage, ClusterManager cluster) throws IOException {
        this.store = storage.coordinatorStore();
        this.api = new CoordinatorApi(store, config.port(), config.tls());
        // A node's liveness is measured against the node->coordinator heartbeat cadence, NOT the
        // coordinator's own cluster heartbeat -- otherwise lowering the coordinator's
        // WIGGLE_HEARTBEAT_INTERVAL_MILLIS could falsely reap live nodes.
        long reconcileMillis = CoordinatorApi.NODE_HEARTBEAT_INTERVAL_SECONDS * 1000L;
        long nodeDeadMillis = CoordinatorApi.nodeDeadMillis(config.missedHeartbeatsBeforeDead());
        this.reconciler = new CoordinatorReconciler(store, cluster::isLeader, reconcileMillis, nodeDeadMillis);
    }

    @Override public void start() {
        api.start();
        reconciler.start();
        LOG.log(System.Logger.Level.INFO, () -> "coordinator control plane on port " + api.port());
    }

    @Override public void close() {
        reconciler.close();
        api.close();
        store.close();
    }

    @Override public int port() { return api.port(); }

    @Override public int dashboardPort() { return -1; }

    @Override public WorkflowEngine engine() {
        throw new IllegalStateException("no engine in coordinator role");
    }
}
