package dev.wiggle.client.dsl;

import dev.wiggle.core.ContextCodec;
import dev.wiggle.core.ExecutionMode;
import dev.wiggle.core.Node;
import dev.wiggle.core.RetryPolicy;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/** Mutable build state shared by a root stream and all of its branch sub-streams. */
final class Pipeline<T> {

    final String name;
    final ContextCodec<T> codec;
    final RetryPolicy defaultRetry;
    final Map<String, Node> nodes = new LinkedHashMap<>();
    final Map<String, ActivityHandler> handlers = new LinkedHashMap<>();
    final Set<String> queues = new LinkedHashSet<>();
    final Set<String> stepNames = new LinkedHashSet<>();
    final Set<String> checkpoints = new LinkedHashSet<>();
    String startNode;
    String defaultQueue;
    ExecutionMode executionMode = ExecutionMode.DEFAULT;
    private int counter;

    Pipeline(String name, ContextCodec<T> codec, RetryPolicy defaultRetry) {
        if (name == null || name.isBlank()) throw new IllegalArgumentException("workflow name is required");
        this.name = name;
        this.codec = codec;
        this.defaultRetry = defaultRetry == null ? RetryPolicy.forever() : defaultRetry;
        this.defaultQueue = name;
    }

    String nextId(String prefix) {
        return prefix + (++counter);
    }

    String activityFor(String stepName) {
        if (stepName == null || stepName.isBlank()) throw new IllegalArgumentException("step name is required");
        if (!stepNames.add(stepName)) {
            throw new IllegalArgumentException("duplicate step name '" + stepName + "' in workflow " + name);
        }
        return name + "#" + stepName;
    }

    void put(Node n) { nodes.put(n.id(), n); }

    Node get(String id) { return nodes.get(id); }

    void wire(String fromId, int slot, String target) {
        Node n = nodes.get(fromId);
        nodes.put(fromId, slot == 0 ? n.withNext(target) : n.withAltNext(target));
    }
}
