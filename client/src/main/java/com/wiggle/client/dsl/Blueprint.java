package com.wiggle.client.dsl;

import com.wiggle.core.ContextCodec;
import com.wiggle.core.WorkflowDefinition;

import java.util.Map;
import java.util.Set;

/**
 * The output of {@code build()}: an immutable topology to register with the server,
 * plus the local handler table the worker dispatches against. One artifact, two
 * audiences -- which is why the DSL can be both the schema and the implementation.
 */
public record Blueprint<T>(WorkflowDefinition definition, Map<String, ActivityHandler> handlers,
                           ContextCodec<T> codec) {

    public Blueprint {
        handlers = Map.copyOf(handlers);
    }

    public String name() { return definition.name(); }

    public int version() { return definition.version(); }

    public Set<String> queues() { return definition.queues(); }
}
