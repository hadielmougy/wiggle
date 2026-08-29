package dev.wiggle.dist.coord;

/**
 * Coordinator link used when {@code WIGGLE_COORDINATOR_URL} is set.
 *
 * <p>Phase 0 stub: it records the target and logs what it <em>would</em> do, so the seam is exercised
 * end to end without a coordinator running. The real Register/Heartbeat/Deregister over the
 * {@code CellCoordinator} gRPC surface (all best-effort, with generation-driven re-fetch and DRAIN
 * handling) lands in Phase 1 (see {@code docs/phase-1-tickets.md}, T7).
 */
public final class HttpCoordinatorLink implements CoordinatorLink {

    private static final System.Logger LOG = System.getLogger(HttpCoordinatorLink.class.getName());

    private final String coordinatorUrl;

    public HttpCoordinatorLink(String coordinatorUrl) {
        this.coordinatorUrl = coordinatorUrl;
    }

    @Override public void register(NodeInfo node) {
        LOG.log(System.Logger.Level.INFO,
                () -> "coordinator link: would register " + node + " with " + coordinatorUrl + " (Phase 1/T7)");
    }

    @Override public void heartbeat() { /* Phase 1/T7 */ }

    @Override public void close() {
        LOG.log(System.Logger.Level.INFO, () -> "coordinator link: would deregister from " + coordinatorUrl);
    }
}
