package dev.wiggle.server.coord;

import dev.wiggle.core.Tls;

import java.io.IOException;

/**
 * A self-hosted coordinator control plane: the {@code CellCoordinator} gRPC service plus a leader-only
 * reconcile/retire loop, over a {@link CoordinatorStore}. It runs no engine, no cluster, no
 * {@code WiggleControlPlane} — it depends on nothing from the cell engine and talks to cells only over
 * gRPC. The store and the {@code isLeader} signal are injected by the composition layer, so the backing
 * (reuse-the-DB today; etcd/Raft later) is a deployment choice.
 */
public final class CoordinatorServer implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(CoordinatorServer.class.getName());

    private final CoordinatorApi api;
    private final CoordinatorReconciler reconciler;
    private final LeaderElection election;

    /**
     * @param store    the coordinator's durable state (built by the composition layer from config)
     * @param port     the gRPC port to serve the CellCoordinator service on
     * @param tls      TLS options for that gRPC server
     * @param missedHeartbeatsBeforeDead reaper threshold for dead cell nodes
     * @param nodeId   this coordinator node's id, for the leader lease (a multi-node coordinator uses it
     *                 to elect a single writer over the store; a lone coordinator just holds the lease)
     */
    public CoordinatorServer(CoordinatorStore store, int port, Tls.Options tls,
                             int missedHeartbeatsBeforeDead, String nodeId) throws IOException {
        this.election = new LeaderElection(store, nodeId);
        // Heartbeats feed the census (api writes) that drives epoch retire (reconciler reads).
        LiveCensus census = new LiveCensus();
        this.api = new CoordinatorApi(store, port, tls, census);
        // A cell's liveness is measured against the node->coordinator heartbeat cadence.
        long reconcileMillis = CoordinatorApi.NODE_HEARTBEAT_INTERVAL_SECONDS * 1000L;
        long nodeDeadMillis = CoordinatorApi.nodeDeadMillis(missedHeartbeatsBeforeDead);
        this.reconciler = new CoordinatorReconciler(store, census, election::isLeader, reconcileMillis, nodeDeadMillis);
    }

    public CoordinatorServer start() {
        election.start();   // acquire the leader lease before the reconcile loop consults it
        api.start();
        reconciler.start();
        LOG.log(System.Logger.Level.INFO, () -> "coordinator control plane on port " + api.port());
        return this;
    }

    public int port() { return api.port(); }

    /** Closes the gRPC service, the reconcile loop, and the leader lease. The store's lifecycle is the caller's. */
    @Override public void close() {
        reconciler.close();
        api.close();
        election.close();
    }
}
