package com.wiggle.server.coord;

/**
 * The substrate-agnostic seam for standing up a cell (R22). Provisioning is expressed as three
 * operations; how they are carried out -- in-process, a forked JVM, or (later) a container/pod -- is
 * the implementation's business. The coordinator (via {@link NamespaceProvisioner}) never touches
 * {@code StorageFactory} or {@code WiggleServer} directly; only a deployer does.
 *
 * <p>{@link #migrateSchema} runs once, before any node serves (R23 seed-before-serving at the schema
 * level). {@link #deploy} brings up the cluster and returns where it can be reached. Both should be
 * idempotent so the provisioner can resume a half-finished namespace.
 */
public interface CellDeployer extends AutoCloseable {

    /** Creates the cell's storage (if needed) and migrates it to the current CELL schema. */
    void migrateSchema(NamespaceSpec spec);

    /** Brings up the cell's cluster and returns a handle to it. */
    Deployment deploy(NamespaceSpec spec);

    /** Tears down a previously-deployed cell. Best-effort; unknown ids are ignored. */
    void teardown(String deploymentId);

    /** A running cell: an opaque id for teardown, and the endpoint clients/the coordinator dial. */
    record Deployment(String id, String endpoint) {}

    /** Releases any resources the deployer itself holds (started cells are torn down separately). */
    @Override default void close() { }
}
