package com.wiggle.client.flow;

import com.wiggle.core.RetryPolicy;

/**
 * The stage a 7-armed {@link Wiggle#allOf} returns; its combine is mandatory, because the arms ran on
 * isolated copies of the context and a combine is the only way their results reach the flow.
 *
 * <p>Arms bind by position: the combine takes one parameter per arm, in the order the handles were
 * given to {@code allOf}. A referenced combine is checked against that shape while the workflow is
 * being defined, so a handler written for a different fan-out is caught there rather than at
 * worker startup.
 */
public final class Fork7<A, B, C, D, E, F, G> {

    private final Plan.Fork fork;

    Fork7(Plan.Fork fork) {
        this.fork = fork;
    }

    /**
     * The merge for the preceding fan-out: a handler taking the arms' results in the order the handles
     * were given to {@code allOf}, and returning the complete post-join context.
     */
    public <R> WiggleFlow<R> combine(FlowFn7<A, B, C, D, E, F, G, R> combine) {
        return merge(StepNames.ofCombine(combine, fork.armNames, false), null, null);
    }

    /**
     * {@link #combine(FlowFn7)} for a merge that also needs the pre-fork context: a handler declared
     * as {@code (@Context X base, ...7 arms)}. The context parameter comes first so the type
     * arguments line up with the declaration.
     */
    public <X, R> WiggleFlow<R> combineWithContext(FlowFn8<X, A, B, C, D, E, F, G, R> combine) {
        return merge(StepNames.ofCombine(combine, fork.armNames, true), null, null);
    }

    /**
     * The merge named explicitly rather than referenced -- the escape hatch for a combine handler this
     * API cannot type, and the only form that skips the arm check above.
     */
    public <R> WiggleFlow<R> combine(String name, Class<R> result) {
        return merge(name, null, null);
    }

    /** {@link #combine(FlowFn7<A,)} with an explicit retry policy for the combine node. */
    public <R> WiggleFlow<R> combine(FlowFn7<A, B, C, D, E, F, G, R> combine, RetryPolicy retry) {
        return merge(StepNames.ofCombine(combine, fork.armNames, false), retry, null);
    }

    /** {@link #combine(FlowFn7<A,)} pinned to a dedicated worker queue. */
    public <R> WiggleFlow<R> combine(FlowFn7<A, B, C, D, E, F, G, R> combine, String queue) {
        return merge(StepNames.ofCombine(combine, fork.armNames, false), null, queue);
    }

    /** {@link #combine(FlowFn7<A,)} with both a retry policy and a dedicated queue. */
    public <R> WiggleFlow<R> combine(FlowFn7<A, B, C, D, E, F, G, R> combine, RetryPolicy retry, String queue) {
        return merge(StepNames.ofCombine(combine, fork.armNames, false), retry, queue);
    }

    /** {@link #combine(FlowFn7<A,)} , queue first. */
    public <R> WiggleFlow<R> combine(FlowFn7<A, B, C, D, E, F, G, R> combine, String queue, RetryPolicy retry) {
        return merge(StepNames.ofCombine(combine, fork.armNames, false), retry, queue);
    }

    /** {@link #combineWithContext(FlowFn8<X,)} with an explicit retry policy for the combine node. */
    public <X, R> WiggleFlow<R> combineWithContext(FlowFn8<X, A, B, C, D, E, F, G, R> combine, RetryPolicy retry) {
        return merge(StepNames.ofCombine(combine, fork.armNames, true), retry, null);
    }

    /** {@link #combineWithContext(FlowFn8<X,)} pinned to a dedicated worker queue. */
    public <X, R> WiggleFlow<R> combineWithContext(FlowFn8<X, A, B, C, D, E, F, G, R> combine, String queue) {
        return merge(StepNames.ofCombine(combine, fork.armNames, true), null, queue);
    }

    /** {@link #combineWithContext(FlowFn8<X,)} with both a retry policy and a dedicated queue. */
    public <X, R> WiggleFlow<R> combineWithContext(FlowFn8<X, A, B, C, D, E, F, G, R> combine, RetryPolicy retry, String queue) {
        return merge(StepNames.ofCombine(combine, fork.armNames, true), retry, queue);
    }

    /** {@link #combineWithContext(FlowFn8<X,)} , queue first. */
    public <X, R> WiggleFlow<R> combineWithContext(FlowFn8<X, A, B, C, D, E, F, G, R> combine, String queue, RetryPolicy retry) {
        return merge(StepNames.ofCombine(combine, fork.armNames, true), retry, queue);
    }

    /** {@link #combine(String, Class)} with an explicit retry policy for the combine node. */
    public <R> WiggleFlow<R> combine(String name, Class<R> result, RetryPolicy retry) {
        return merge(name, retry, null);
    }

    /** {@link #combine(String, Class)} pinned to a dedicated worker queue. */
    public <R> WiggleFlow<R> combine(String name, Class<R> result, String queue) {
        return merge(name, null, queue);
    }

    /** {@link #combine(String, Class)} with both a retry policy and a dedicated queue. */
    public <R> WiggleFlow<R> combine(String name, Class<R> result, RetryPolicy retry, String queue) {
        return merge(name, retry, queue);
    }

    /** {@link #combine(String, Class)} , queue first. */
    public <R> WiggleFlow<R> combine(String name, Class<R> result, String queue, RetryPolicy retry) {
        return merge(name, retry, queue);
    }

    private <R> WiggleFlow<R> merge(String name, RetryPolicy retry, String queue) {
        fork.combine(name, retry, queue);
        return new WiggleFlow<>(fork);
    }
}
