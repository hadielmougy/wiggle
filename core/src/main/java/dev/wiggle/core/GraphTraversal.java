package dev.wiggle.core;

import java.util.Set;

/**
 * Pure graph-walking decisions shared by the server's state machine and the worker's local
 * driver, so the two can never disagree about "what runs next" or "where must we hand back".
 * No I/O, no DB -- just the compiled graph model.
 */
public final class GraphTraversal {

    private GraphTraversal() {}

    /** Why a worker cannot keep running the next node locally; {@code null} means it can. */
    public enum Handback { SLEEP, FORK, JOIN, USER_TASK, OTHER_QUEUE, TERMINAL }

    /** The successor of a completed worker step: task -> {@code next}; predicate -> next/altNext. */
    public static String successor(Node node, boolean predicateValue) {
        return node.kind() == NodeKind.PREDICATE
                ? (predicateValue ? node.next() : node.altNext())
                : node.next();
    }

    /**
     * Whether a worker serving {@code workerQueues} can execute {@code next} locally as part of a
     * chain, or the reason it must hand control back to the server.
     *
     * @return {@code null} if {@code next} is a same-queue TASK/PREDICATE the worker can run now;
     *         otherwise the {@link Handback} reason.
     */
    public static Handback classify(Node next, Set<String> workerQueues) {
        return switch (next.kind()) {
            case TASK, PREDICATE -> workerQueues.contains(next.queue()) ? null : Handback.OTHER_QUEUE;
            case SLEEP -> Handback.SLEEP;
            case FORK -> Handback.FORK;
            case JOIN -> Handback.JOIN;
            case USER_TASK -> Handback.USER_TASK;
            case END -> Handback.TERMINAL;
        };
    }
}
