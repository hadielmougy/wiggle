package dev.wiggle.core;

import java.util.List;

/**
 * A node before it has an id: the DSL describes a node's kind and payload, and the builder
 * turns it into a positioned {@link Node} via {@link #toNode(String)} once it assigns the id.
 * Edges are wired on the resulting {@code Node}, not here -- a draft is inert.
 */
public record NodeDraft(NodeKind kind, String name, String activity, String queue,
                        RetryPolicy retry, long sleepMillis, int expected, boolean success,
                        String reason, String itemsKey, String itemKey) {

    public static NodeDraft task(String name, String activity, String queue, RetryPolicy retry) {
        return new NodeDraft(NodeKind.TASK, name, activity, queue, retry, 0, 0, false, null, null, null);
    }

    public static NodeDraft predicate(String name, String activity, String queue, RetryPolicy retry) {
        return new NodeDraft(NodeKind.PREDICATE, name, activity, queue, retry, 0, 0, false, null, null, null);
    }

    public static NodeDraft sleep(String name, long millis) {
        return new NodeDraft(NodeKind.SLEEP, name, null, null, null, millis, 0, false, null, null, null);
    }

    public static NodeDraft fork(String name) {
        return new NodeDraft(NodeKind.FORK, name, null, null, null, 0, 0, false, null, null, null);
    }

    public static NodeDraft dynFork(String name, String itemsKey, String itemKey) {
        return new NodeDraft(NodeKind.DYN_FORK, name, null, null, null, 0, 0, false, null, itemsKey, itemKey);
    }

    public static NodeDraft join(String name, int expected) {
        return new NodeDraft(NodeKind.JOIN, name, null, null, null, 0, expected, false, null, null, null);
    }

    public static NodeDraft signal(String name, long deadlineMillis) {
        return new NodeDraft(NodeKind.SIGNAL, name, null, null, null, deadlineMillis, 0, false, null, null, null);
    }

    public static NodeDraft subWorkflow(String name, String workflow) {
        return new NodeDraft(NodeKind.SUB_WORKFLOW, name, workflow, null, null, 0, 0, false, null, null, null);
    }

    public static NodeDraft end(boolean success, String reason) {
        return new NodeDraft(NodeKind.END, "end", null, null, null, 0, 0, success, reason, null, null);
    }

    /** Positions this draft at {@code id}, producing an edge-less {@link Node} ready to be wired. */
    public Node toNode(String id) {
        return new Node(id, kind, name, activity, queue, retry, sleepMillis, null, null,
                List.of(), expected, success, reason, itemsKey, itemKey);
    }
}
