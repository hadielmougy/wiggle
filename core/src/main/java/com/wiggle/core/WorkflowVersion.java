package com.wiggle.core;

/**
 * One registered version of one workflow -- the unit a worker can be allocated.
 *
 * <p>A worker that binds handlers without a version serves every version of that workflow, which is
 * the default and almost always what you want: step names are stable across versions, so one
 * implementation covers them all. Naming a version instead <em>allocates</em> that version to that
 * worker: it validates its handlers against that exact graph, and claims only its tasks.
 *
 * <p>That is what makes a capability move between services cleanly. Service A serves v1; v2 is
 * published and service B serves it; A drains its in-flight v1 instances and retires. Without the
 * scoping, A would go on claiming v2's tasks and running them with v1's code -- the activity a
 * handler binds is {@code workflow#step}, which carries no version, so the names match and nothing
 * notices.
 */
public record WorkflowVersion(String workflow, int version) {

    public WorkflowVersion {
        if (workflow == null || workflow.isBlank()) {
            throw new IllegalArgumentException("workflow is required");
        }
    }

    /** The {@code name:version} form used as a map key for compiled graphs. */
    public String key() {
        return workflow + ":" + version;
    }

    @Override public String toString() {
        return key();
    }
}
