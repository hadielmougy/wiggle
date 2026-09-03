package com.wiggle.client.dsl;

import com.wiggle.core.WorkflowDefinition;

import java.util.Set;

/**
 * The output of {@code build()}: an immutable workflow topology (nodes, edges, kinds, queues, retry)
 * to register with the server. It carries no step logic and no context type -- the graph is the
 * whole artifact. Step implementations live in {@link com.wiggle.client.worker.Handlers @Handlers}
 * classes bound on a worker and matched to the graph by name.
 */
public record Blueprint(WorkflowDefinition definition) {

    public String name() { return definition.name(); }

    public int version() { return definition.version(); }

    public Set<String> queues() { return definition.queues(); }
}
