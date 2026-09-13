package com.wiggle.client.flow;

/**
 * The stage a 8-armed {@link Wiggle#allOf} returns; its combine is mandatory, because the arms ran on
 * isolated copies of the context and a combine is the only way their results reach the flow.
 *
 * <p>A referenced combine is checked against the fan-out while the workflow is being defined: if the
 * handler's {@link com.wiggle.client.worker.Arm @Arm} parameters name the arms, they must name
 * <em>these</em> arms, in the order the handles were given to {@code allOf}. A handler with no
 * {@code @Arm} at all binds by position, which its own signature already fixes.
 */
public final class Fork8<A, B, C, D, E, F, G, H> {

    private final Plan.Fork fork;

    Fork8(Plan.Fork fork) {
        this.fork = fork;
    }

    /**
     * The merge for the preceding fan-out: a handler taking the arms' results in the order the handles
     * were given to {@code allOf}, and returning the complete post-join context.
     */
    public <R> WiggleFlow<R> combine(FlowFn8<A, B, C, D, E, F, G, H, R> combine) {
        return merge(StepNames.ofCombine(combine, fork.armNames, false));
    }

    /**
     * {@link #combine(FlowFn8)} for a merge that also needs the pre-fork context: a handler declared
     * as {@code (@Context X base, ...8 arms)}. The context parameter comes first so the type
     * arguments line up with the declaration.
     */
    public <X, R> WiggleFlow<R> combineWithContext(FlowFn9<X, A, B, C, D, E, F, G, H, R> combine) {
        return merge(StepNames.ofCombine(combine, fork.armNames, true));
    }

    /**
     * The merge named explicitly rather than referenced -- the escape hatch for a combine handler this
     * API cannot type, and the only form that skips the arm check above.
     */
    public <R> WiggleFlow<R> combine(String name, Class<R> result) {
        return merge(name);
    }

    private <R> WiggleFlow<R> merge(String name) {
        fork.combineName = name;
        return new WiggleFlow<>(fork);
    }
}
