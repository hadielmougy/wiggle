package dev.wiggle.server;

import dev.wiggle.server.cluster.ClusterManager;
import dev.wiggle.server.engine.WorkflowEngine;
import dev.wiggle.server.store.Storage;

/**
 * The {@link ServerRole#COORDINATOR} subsystems. A coordinator runs no engine, no dispatch, and no
 * {@code WiggleControlPlane}; it reuses the shared storage + {@link ClusterManager} (election) and
 * will serve the {@code CellCoordinator} control plane plus a leader-only reconcile loop.
 *
 * <p>Phase 0 stub: the composition seam exists and a coordinator node starts cleanly (storage +
 * cluster only). The gRPC service and reconciler are wired in Phase 1 (see
 * {@code docs/phase-1-tickets.md}, T6).
 */
final class CoordinatorBundle implements ServerBundle {

    private static final System.Logger LOG = System.getLogger(CoordinatorBundle.class.getName());

    private final int port;

    CoordinatorBundle(ServerConfig config, Storage storage, ClusterManager cluster) {
        this.port = config.port();
        // Phase 1/T6 wires the CellCoordinator gRPC service + reconcile loop here,
        // sharing `storage` (as a CoordinatorStore) and `cluster` (leader election).
    }

    @Override public void start() {
        LOG.log(System.Logger.Level.INFO,
                () -> "coordinator role: control plane not yet implemented (Phase 1/T6); "
                        + "storage + election are up");
    }

    @Override public void close() { }

    @Override public int port() { return port; }

    @Override public int dashboardPort() { return -1; }

    @Override public WorkflowEngine engine() {
        throw new IllegalStateException("no engine in coordinator role");
    }
}
