package com.wiggle.core;

import java.util.List;

/**
 * A branch a step created at run time with {@code Step.create(input)}: the branch's whole context,
 * an optional key that makes it addressable in the combine, and the chain of steps it runs. It
 * rides the step's report and is committed in the transaction that settles the step.
 */
public record CreatedBranch(Object input, String key, List<BranchStep> steps) {

    /** The prefix of the id of every node a created branch compiles to. No compiled node id has it. */
    public static final String NODE_PREFIX = "~";

    public CreatedBranch {
        steps = steps == null ? List.of() : List.copyOf(steps);
    }

    /** Whether {@code nodeId} names a node a created branch compiled to. */
    public static boolean isCreatedNode(String nodeId) {
        return nodeId != null && nodeId.startsWith(NODE_PREFIX);
    }

    /**
     * One step of a created branch. {@code kind} is {@link NodeKind#TASK}, {@link NodeKind#PREDICATE}
     * or {@link NodeKind#SLEEP}. A null {@code queue} or {@code retry} takes the creating step's.
     */
    public record BranchStep(String name, NodeKind kind, boolean compensable, String queue,
                             RetryPolicy retry, long sleepMillis) {

        public static BranchStep step(String name, NodeKind kind, boolean compensable, String queue,
                                      RetryPolicy retry) {
            return new BranchStep(name, kind, compensable, queue, retry, 0);
        }

        public static BranchStep sleep(long millis) {
            return new BranchStep(null, NodeKind.SLEEP, false, null, null, millis);
        }
    }
}
