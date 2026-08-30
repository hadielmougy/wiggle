package com.wiggle.server;

import com.wiggle.server.engine.WorkflowEngine;

/**
 * The cell subsystems of a {@link WiggleServer}. Everything a node runs <em>beyond</em> the shared
 * storage + {@link com.wiggle.server.cluster.ClusterManager} lives in a bundle ({@link CellBundle}: the
 * engine + control plane). (The seam predates the coordinator's extraction into its own module; it is
 * kept for the placement/engine accessors.)
 */
interface ServerBundle {

    /** Starts the bundle's subsystems. The shared cluster is already started by {@link WiggleServer}. */
    void start();

    /** Stops the bundle's subsystems. The shared cluster/storage are closed by {@link WiggleServer}. */
    void close();

    /** The gRPC port this node serves on. */
    int port();

    /** The dashboard port, or {@code -1} if none. */
    int dashboardPort();

    /** The workflow engine, for embedders/tests. Throws for roles that run no engine. */
    WorkflowEngine engine();

    /**
     * The coordinator-managed placement (mint epoch + owned shards), or {@code null} for a standalone
     * cell and for the coordinator role. The coordinator link re-points it when the policy changes.
     */
    default CellPlacement placement() { return null; }
}
