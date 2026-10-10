package com.wiggle.core;

import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Pure graph-walking decisions shared by the server's state machine and the worker's local
 * driver, so the two can never disagree about "what runs next" or "where must we hand back".
 * No I/O, no DB -- just the compiled graph model.
 */
public final class GraphTraversal {

    private GraphTraversal() {}

    /** Why a worker cannot keep running the next node locally; {@code null} means it can. */
    public enum Handback { SLEEP, FORK, JOIN, SIGNAL, SUB_WORKFLOW, OTHER_QUEUE, TERMINAL }

    /** The successor of a completed worker step: task -> {@code next}; predicate -> next/altNext. */
    public static String successor(Node node, boolean predicateValue) {
        return node.kind() == NodeKind.PREDICATE
                ? (predicateValue ? node.next() : node.altNext())
                : node.next();
    }

    /**
     * The combine a step that creates branches at run time feeds, or empty when {@code node} is not
     * such a step: a task followed directly by a dynamic join and the combine collecting under
     * {@link ScratchKeys#spawn} of the task's name.
     */
    public static Optional<Node> spawnCombine(Node node, Function<String, Optional<Node>> lookup) {
        if (node.kind() != NodeKind.TASK || node.isCombine() || node.next() == null) return Optional.empty();
        return lookup.apply(node.next())
                .filter(join -> join.kind() == NodeKind.JOIN && join.expected() == 0 && join.next() != null)
                .flatMap(join -> lookup.apply(join.next()))
                .filter(c -> c.isCombine() && ScratchKeys.spawn(node.name()).equals(c.collectKey()));
    }

    /** Whether {@code node} may create branches: a step followed by its combine, or that combine. */
    public static boolean mayCreateBranches(Node node, Function<String, Optional<Node>> lookup) {
        return (node.isCombine() && ScratchKeys.isSpawn(node.collectKey()))
                || spawnCombine(node, lookup).isPresent();
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
            case FORK, DYN_FORK -> Handback.FORK;
            case JOIN -> Handback.JOIN;
            case SIGNAL -> Handback.SIGNAL;
            case SUB_WORKFLOW -> Handback.SUB_WORKFLOW;
            case END -> Handback.TERMINAL;
        };
    }
}
