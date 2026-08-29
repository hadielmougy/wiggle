package dev.wiggle.server;

import dev.wiggle.server.engine.WorkflowEngine;

/**
 * The role-specific subsystems of a {@link WiggleServer}. Everything a node runs <em>beyond</em> the
 * shared storage + {@link dev.wiggle.server.cluster.ClusterManager} lives in a bundle, so a node
 * composes the right subsystems for its {@link ServerRole} rather than branching behaviour: a
 * {@link CellBundle} runs the engine + control plane, a {@link CoordinatorBundle} runs the
 * coordinator surface (and no engine).
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
}
