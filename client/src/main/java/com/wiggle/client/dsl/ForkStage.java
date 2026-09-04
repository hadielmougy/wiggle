package com.wiggle.client.dsl;

import java.util.List;

/**
 * The mandatory stage after a {@link WorkflowBuilder#fork}: the branches fanned out, each on its own
 * isolated copy of the context, and now must be rejoined by an explicit {@link #combine}. There is
 * no implicit merge -- a branch's writes never touch the shared context, so the only way a branch's
 * result reaches the flow is through the combine.
 *
 * <p>{@code combine} is topology only: it declares the combine node (by name) and records the arm
 * names. The merge itself is a handler bound on the worker -- a method named the same as the combine
 * whose {@link com.wiggle.client.worker.Arm @Arm} parameters receive each branch's typed result --
 * or, if no such method exists, the worker's default union (fold all arms). The fork left the stream
 * with no open end, so a forgotten combine also fails at {@code build()}.
 *
 * @see WorkflowBuilder#fork
 */
public final class ForkStage {

    private final WorkflowBuilder stream;
    private final List<Branch> branches;
    private boolean combined;

    ForkStage(WorkflowBuilder stream, List<Branch> branches) {
        this.stream = stream;
        this.branches = branches;
    }

    /**
     * The mandatory merge for the preceding fork. Declares the combine node named {@code name}; each
     * branch's result is later handed to the matching worker handler keyed by branch name (or folded
     * by the default union). Resumes the normal step flow at the combine node.
     *
     * @param name the step name of the combine node (must be unique in the workflow)
     * @return the stream, reopened after the combine node
     */
    public WorkflowBuilder combine(String name) {
        if (combined) throw new IllegalStateException("combine already applied to this fork");
        combined = true;
        stream.buildForkCombine(branches, name);
        return stream;
    }
}
