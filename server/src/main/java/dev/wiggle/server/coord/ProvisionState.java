package dev.wiggle.server.coord;

/**
 * The lifecycle of a namespace being provisioned into a cell (a database + a Wiggle cluster). The
 * happy path is {@code REQUESTED → MIGRATING_SCHEMA → STARTING → ACTIVE}; any step that throws leaves
 * the record {@link #FAILED}, from which {@link NamespaceProvisioner#create} resumes on the next call.
 */
public enum ProvisionState {
    /** Recorded, nothing done yet. */                         REQUESTED,
    /** Storage created and schema migrated (before serving). */ MIGRATING_SCHEMA,
    /** The cell cluster is being deployed. */                 STARTING,
    /** Healthy; the cell endpoint is recorded and resolvable. */ ACTIVE,
    /** A step failed; the record is resumable by re-running create. */ FAILED
}
