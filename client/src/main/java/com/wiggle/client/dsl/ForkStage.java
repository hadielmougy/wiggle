package com.wiggle.client.dsl;

import java.util.List;
import java.util.Objects;

/**
 * The mandatory stage after a {@link WorkflowStream#fork}: the branches fanned out, each on its own
 * isolated copy of the context, and now must be rejoined by an explicit {@link #combine}. There is
 * no implicit merge -- a branch's writes never touch the shared context, so the only way a branch's
 * result reaches the flow is through the aggregator you supply here.
 *
 * <p>{@code combine} exposes exactly one operation, so the type system forces every fork to be
 * followed by a merge, and the merge is the only thing that reopens the stream for normal chaining.
 * The fork left the stream with no open end, so a forgotten combine also fails at {@code build()}.
 *
 * @see WorkflowStream#fork
 */
public final class ForkStage {

    private final WorkflowStream stream;
    private final List<Branch> branches;
    private boolean combined;

    ForkStage(WorkflowStream stream, List<Branch> branches) {
        this.stream = stream;
        this.branches = branches;
    }

    /**
     * The mandatory merge for the preceding fork. Each branch's accumulated result is handed to
     * {@code combine} keyed by its {@link Branch#name() name}; the return value is merged into the
     * context and the normal step flow resumes at the combine node.
     *
     * @param name    the step name of the combine node (must be unique in the workflow)
     * @param combine the manual, order-independent merge of the branch results
     * @return the stream, reopened after the combine node
     */
    public WorkflowStream combine(String name, Aggregator combine) {
        if (combined) throw new IllegalStateException("combine already applied to this fork");
        Objects.requireNonNull(combine, "combine");
        combined = true;
        stream.buildForkCombine(branches, name, combine);
        return stream;
    }
}