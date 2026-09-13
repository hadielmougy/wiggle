package com.wiggle.client.flow;

/**
 * The stage a 3-armed {@link Wiggle#allOf} returns; its combine is mandatory, because the arms ran on
 * isolated copies of the context and a combine is the only way their results reach the flow.
 *
 * <p>Arms bind by position: the combine takes one parameter per arm, in the order the handles were
 * given to {@code allOf}. A referenced combine is checked against that shape while the workflow is
 * being defined, so a handler written for a different fan-out is caught there rather than at
 * worker startup.
 */
public final class Fork3<A, B, C> {

    private final Plan.Fork fork;

    Fork3(Plan.Fork fork) {
        this.fork = fork;
    }

    /**
     * The merge for the preceding fan-out: a handler taking the arms' results in the order the handles
     * were given to {@code allOf}, and returning the complete post-join context.
     */
    public <R> WiggleFlow<R> combine(FlowFn3<A, B, C, R> combine) {
        return merge(StepNames.ofCombine(combine, fork.armNames, false));
    }

    /**
     * {@link #combine(FlowFn3)} for a merge that also needs the pre-fork context: a handler declared
     * as {@code (@Context X base, ...3 arms)}. The context parameter comes first so the type
     * arguments line up with the declaration.
     */
    public <X, R> WiggleFlow<R> combineWithContext(FlowFn4<X, A, B, C, R> combine) {
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
