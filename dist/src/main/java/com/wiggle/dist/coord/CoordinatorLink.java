package com.wiggle.dist.coord;

import com.wiggle.server.CellPlacement;

import java.util.Map;
import java.util.function.Supplier;

/**
 * A node's optional link to the cell coordinator: announce this node, keep it live, and deregister on
 * shutdown. Every method is best-effort -- a coordinator that is absent, down, or flapping must never
 * stop the node from serving. {@link NoopCoordinatorLink} is used when no coordinator is configured.
 */
public interface CoordinatorLink extends AutoCloseable {

    /**
     * Announce this node to the coordinator (for discovery/health). When {@code runtime} is non-null the
     * link applies the placement the coordinator returns (so the node mints correctly-sharded ids) and
     * reports its live-by-epoch census on every heartbeat (so drained epochs can be retired). Pass
     * {@code null} for a node that runs no cell (the coordinator role) or has no namespace.
     */
    void register(NodeInfo node, CellRuntime runtime);

    /** Liveness ping; observing a newer config generation is how change propagates (T7). */
    void heartbeat();

    /** Re-fetch and apply this node's placement now (best-effort). Used as the cell's on-demand
     *  self-heal when a mint hits standby after an epoch bump. No-op without a coordinator. */
    default void refreshPlacement() { }

    /** Deregister on shutdown. */
    @Override void close();

    /** What a node announces about itself. {@code cellFingerprint} identifies the cell's shared storage
     *  (null when the backend has none), letting the coordinator reject a reused cell id. */
    record NodeInfo(String nodeName, String namespace, String cellId, String endpoint, String engineVersion,
                    String cellFingerprint) {}

    /** The cell-side handles the link needs: where to apply placement, and how to read live counts. */
    record CellRuntime(CellPlacement placement, Supplier<Map<Long, Integer>> liveByEpoch) {}
}
