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
     * One step of a created branch:
     * <ul>
     *   <li>{@link NodeKind#TASK} or {@link NodeKind#PREDICATE}: a worker step named {@code name}. A
     *       task with a {@code combine} creates branches of its own, which that combine collects on
     *       the task's queue and retry policy;</li>
     *   <li>{@link NodeKind#SLEEP}: a timer of {@code sleepMillis};</li>
     *   <li>{@link NodeKind#SIGNAL}: a wait for the signal {@code name}, with an optional deadline in
     *       {@code sleepMillis} and an {@code escalation} chain run when it passes;</li>
     *   <li>{@link NodeKind#SUB_WORKFLOW}: the workflow {@code workflow} run as a child, under the node
     *       name {@code name};</li>
     *   <li>{@link NodeKind#FORK}: the {@code arms}, each a chain run on its own copy of the context,
     *       merged by {@code combine}.</li>
     * </ul>
     * A null {@code queue} or {@code retry} takes the creating step's.
     */
    public record BranchStep(String name, NodeKind kind, boolean compensable, String queue,
                             RetryPolicy retry, long sleepMillis, String combine, String workflow,
                             List<BranchStep> escalation, List<List<BranchStep>> arms) {

        public BranchStep {
            escalation = escalation == null ? List.of() : List.copyOf(escalation);
            arms = arms == null ? List.of() : arms.stream().map(List::copyOf).toList();
        }

        public BranchStep(String name, NodeKind kind, boolean compensable, String queue,
                          RetryPolicy retry, long sleepMillis, String combine) {
            this(name, kind, compensable, queue, retry, sleepMillis, combine, null, List.of(), List.of());
        }

        public BranchStep(String name, NodeKind kind, boolean compensable, String queue,
                          RetryPolicy retry, long sleepMillis) {
            this(name, kind, compensable, queue, retry, sleepMillis, null);
        }

        public static BranchStep step(String name, NodeKind kind, boolean compensable, String queue,
                                      RetryPolicy retry) {
            return new BranchStep(name, kind, compensable, queue, retry, 0);
        }

        public static BranchStep sleep(long millis) {
            return new BranchStep(null, NodeKind.SLEEP, false, null, null, millis);
        }

        /** A wait for {@code signal}; {@code timeoutMillis == 0} waits for ever. */
        public static BranchStep await(String signal, long timeoutMillis, List<BranchStep> escalation) {
            return new BranchStep(signal, NodeKind.SIGNAL, false, null, null, timeoutMillis, null, null,
                    escalation, List.of());
        }

        public static BranchStep subFlow(String node, String workflow) {
            return new BranchStep(node, NodeKind.SUB_WORKFLOW, false, null, null, 0, null, workflow,
                    List.of(), List.of());
        }

        public static BranchStep fork(List<List<BranchStep>> arms, String combine) {
            return new BranchStep(null, NodeKind.FORK, false, null, null, 0, combine, null, List.of(), arms);
        }

        /** This step, creating branches of its own that {@code combineName} collects. */
        public BranchStep withCombine(String combineName) {
            return new BranchStep(name, kind, compensable, queue, retry, sleepMillis, combineName, workflow,
                    escalation, arms);
        }
    }
}
