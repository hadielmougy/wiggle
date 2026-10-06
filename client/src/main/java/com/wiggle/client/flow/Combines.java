package com.wiggle.client.flow;

import com.wiggle.core.RetryPolicy;

/**
 * The mandatory merge after a fan-out: {@link Wiggle#allOf}, {@link WiggleFlow#thenForEach}. The
 * arms or items ran on isolated copies of the context, so a combine is the only way their results
 * reach the flow; its return is the complete post-join context.
 *
 * <p>A combine's parameters are found by their type, in any order, on the worker that binds it: a
 * parameter takes the result whose step produces its type, a {@code List}, {@code Set} or
 * {@code Map} takes every result of its element type, and a parameter no result matches receives
 * the context from before the fan-out. Results of the same type go to the parameters of that type in
 * declaration order. A combine need not take every result.
 */
public abstract class Combines {

    Combines() {}

    abstract <R> WiggleFlow<R> merge(String name, RetryPolicy retry, String queue);

    /** A merge handler of 1 parameter, found by type in any order. */
    public <P1, R> WiggleFlow<R> combine(FlowFn<P1, R> combine) {
        return merge(StepNames.of(combine), null, null);
    }

    /** With an explicit retry policy for the combine node. */
    public <P1, R> WiggleFlow<R> combine(FlowFn<P1, R> combine, RetryPolicy retry) {
        return merge(StepNames.of(combine), retry, null);
    }

    /** Pinned to a dedicated worker queue. */
    public <P1, R> WiggleFlow<R> combine(FlowFn<P1, R> combine, String queue) {
        return merge(StepNames.of(combine), null, queue);
    }

    /** With both a retry policy and a dedicated queue. */
    public <P1, R> WiggleFlow<R> combine(FlowFn<P1, R> combine, RetryPolicy retry, String queue) {
        return merge(StepNames.of(combine), retry, queue);
    }

    /** With both, queue first. */
    public <P1, R> WiggleFlow<R> combine(FlowFn<P1, R> combine, String queue, RetryPolicy retry) {
        return merge(StepNames.of(combine), retry, queue);
    }

    /** A merge handler of 2 parameters, found by type in any order. */
    public <P1, P2, R> WiggleFlow<R> combine(FlowFn2<P1, P2, R> combine) {
        return merge(StepNames.of(combine), null, null);
    }

    /** With an explicit retry policy for the combine node. */
    public <P1, P2, R> WiggleFlow<R> combine(FlowFn2<P1, P2, R> combine, RetryPolicy retry) {
        return merge(StepNames.of(combine), retry, null);
    }

    /** Pinned to a dedicated worker queue. */
    public <P1, P2, R> WiggleFlow<R> combine(FlowFn2<P1, P2, R> combine, String queue) {
        return merge(StepNames.of(combine), null, queue);
    }

    /** With both a retry policy and a dedicated queue. */
    public <P1, P2, R> WiggleFlow<R> combine(FlowFn2<P1, P2, R> combine, RetryPolicy retry, String queue) {
        return merge(StepNames.of(combine), retry, queue);
    }

    /** With both, queue first. */
    public <P1, P2, R> WiggleFlow<R> combine(FlowFn2<P1, P2, R> combine, String queue, RetryPolicy retry) {
        return merge(StepNames.of(combine), retry, queue);
    }

    /** A merge handler of 3 parameters, found by type in any order. */
    public <P1, P2, P3, R> WiggleFlow<R> combine(FlowFn3<P1, P2, P3, R> combine) {
        return merge(StepNames.of(combine), null, null);
    }

    /** With an explicit retry policy for the combine node. */
    public <P1, P2, P3, R> WiggleFlow<R> combine(FlowFn3<P1, P2, P3, R> combine, RetryPolicy retry) {
        return merge(StepNames.of(combine), retry, null);
    }

    /** Pinned to a dedicated worker queue. */
    public <P1, P2, P3, R> WiggleFlow<R> combine(FlowFn3<P1, P2, P3, R> combine, String queue) {
        return merge(StepNames.of(combine), null, queue);
    }

    /** With both a retry policy and a dedicated queue. */
    public <P1, P2, P3, R> WiggleFlow<R> combine(FlowFn3<P1, P2, P3, R> combine, RetryPolicy retry, String queue) {
        return merge(StepNames.of(combine), retry, queue);
    }

    /** With both, queue first. */
    public <P1, P2, P3, R> WiggleFlow<R> combine(FlowFn3<P1, P2, P3, R> combine, String queue, RetryPolicy retry) {
        return merge(StepNames.of(combine), retry, queue);
    }

    /** A merge handler of 4 parameters, found by type in any order. */
    public <P1, P2, P3, P4, R> WiggleFlow<R> combine(FlowFn4<P1, P2, P3, P4, R> combine) {
        return merge(StepNames.of(combine), null, null);
    }

    /** With an explicit retry policy for the combine node. */
    public <P1, P2, P3, P4, R> WiggleFlow<R> combine(FlowFn4<P1, P2, P3, P4, R> combine, RetryPolicy retry) {
        return merge(StepNames.of(combine), retry, null);
    }

    /** Pinned to a dedicated worker queue. */
    public <P1, P2, P3, P4, R> WiggleFlow<R> combine(FlowFn4<P1, P2, P3, P4, R> combine, String queue) {
        return merge(StepNames.of(combine), null, queue);
    }

    /** With both a retry policy and a dedicated queue. */
    public <P1, P2, P3, P4, R> WiggleFlow<R> combine(FlowFn4<P1, P2, P3, P4, R> combine, RetryPolicy retry, String queue) {
        return merge(StepNames.of(combine), retry, queue);
    }

    /** With both, queue first. */
    public <P1, P2, P3, P4, R> WiggleFlow<R> combine(FlowFn4<P1, P2, P3, P4, R> combine, String queue, RetryPolicy retry) {
        return merge(StepNames.of(combine), retry, queue);
    }

    /** A merge handler of 5 parameters, found by type in any order. */
    public <P1, P2, P3, P4, P5, R> WiggleFlow<R> combine(FlowFn5<P1, P2, P3, P4, P5, R> combine) {
        return merge(StepNames.of(combine), null, null);
    }

    /** With an explicit retry policy for the combine node. */
    public <P1, P2, P3, P4, P5, R> WiggleFlow<R> combine(FlowFn5<P1, P2, P3, P4, P5, R> combine, RetryPolicy retry) {
        return merge(StepNames.of(combine), retry, null);
    }

    /** Pinned to a dedicated worker queue. */
    public <P1, P2, P3, P4, P5, R> WiggleFlow<R> combine(FlowFn5<P1, P2, P3, P4, P5, R> combine, String queue) {
        return merge(StepNames.of(combine), null, queue);
    }

    /** With both a retry policy and a dedicated queue. */
    public <P1, P2, P3, P4, P5, R> WiggleFlow<R> combine(FlowFn5<P1, P2, P3, P4, P5, R> combine, RetryPolicy retry, String queue) {
        return merge(StepNames.of(combine), retry, queue);
    }

    /** With both, queue first. */
    public <P1, P2, P3, P4, P5, R> WiggleFlow<R> combine(FlowFn5<P1, P2, P3, P4, P5, R> combine, String queue, RetryPolicy retry) {
        return merge(StepNames.of(combine), retry, queue);
    }

    /** A merge handler of 6 parameters, found by type in any order. */
    public <P1, P2, P3, P4, P5, P6, R> WiggleFlow<R> combine(FlowFn6<P1, P2, P3, P4, P5, P6, R> combine) {
        return merge(StepNames.of(combine), null, null);
    }

    /** With an explicit retry policy for the combine node. */
    public <P1, P2, P3, P4, P5, P6, R> WiggleFlow<R> combine(FlowFn6<P1, P2, P3, P4, P5, P6, R> combine, RetryPolicy retry) {
        return merge(StepNames.of(combine), retry, null);
    }

    /** Pinned to a dedicated worker queue. */
    public <P1, P2, P3, P4, P5, P6, R> WiggleFlow<R> combine(FlowFn6<P1, P2, P3, P4, P5, P6, R> combine, String queue) {
        return merge(StepNames.of(combine), null, queue);
    }

    /** With both a retry policy and a dedicated queue. */
    public <P1, P2, P3, P4, P5, P6, R> WiggleFlow<R> combine(FlowFn6<P1, P2, P3, P4, P5, P6, R> combine, RetryPolicy retry, String queue) {
        return merge(StepNames.of(combine), retry, queue);
    }

    /** With both, queue first. */
    public <P1, P2, P3, P4, P5, P6, R> WiggleFlow<R> combine(FlowFn6<P1, P2, P3, P4, P5, P6, R> combine, String queue, RetryPolicy retry) {
        return merge(StepNames.of(combine), retry, queue);
    }

    /** A merge handler of 7 parameters, found by type in any order. */
    public <P1, P2, P3, P4, P5, P6, P7, R> WiggleFlow<R> combine(FlowFn7<P1, P2, P3, P4, P5, P6, P7, R> combine) {
        return merge(StepNames.of(combine), null, null);
    }

    /** With an explicit retry policy for the combine node. */
    public <P1, P2, P3, P4, P5, P6, P7, R> WiggleFlow<R> combine(FlowFn7<P1, P2, P3, P4, P5, P6, P7, R> combine, RetryPolicy retry) {
        return merge(StepNames.of(combine), retry, null);
    }

    /** Pinned to a dedicated worker queue. */
    public <P1, P2, P3, P4, P5, P6, P7, R> WiggleFlow<R> combine(FlowFn7<P1, P2, P3, P4, P5, P6, P7, R> combine, String queue) {
        return merge(StepNames.of(combine), null, queue);
    }

    /** With both a retry policy and a dedicated queue. */
    public <P1, P2, P3, P4, P5, P6, P7, R> WiggleFlow<R> combine(FlowFn7<P1, P2, P3, P4, P5, P6, P7, R> combine, RetryPolicy retry, String queue) {
        return merge(StepNames.of(combine), retry, queue);
    }

    /** With both, queue first. */
    public <P1, P2, P3, P4, P5, P6, P7, R> WiggleFlow<R> combine(FlowFn7<P1, P2, P3, P4, P5, P6, P7, R> combine, String queue, RetryPolicy retry) {
        return merge(StepNames.of(combine), retry, queue);
    }

    /** A merge handler of 8 parameters, found by type in any order. */
    public <P1, P2, P3, P4, P5, P6, P7, P8, R> WiggleFlow<R> combine(FlowFn8<P1, P2, P3, P4, P5, P6, P7, P8, R> combine) {
        return merge(StepNames.of(combine), null, null);
    }

    /** With an explicit retry policy for the combine node. */
    public <P1, P2, P3, P4, P5, P6, P7, P8, R> WiggleFlow<R> combine(FlowFn8<P1, P2, P3, P4, P5, P6, P7, P8, R> combine, RetryPolicy retry) {
        return merge(StepNames.of(combine), retry, null);
    }

    /** Pinned to a dedicated worker queue. */
    public <P1, P2, P3, P4, P5, P6, P7, P8, R> WiggleFlow<R> combine(FlowFn8<P1, P2, P3, P4, P5, P6, P7, P8, R> combine, String queue) {
        return merge(StepNames.of(combine), null, queue);
    }

    /** With both a retry policy and a dedicated queue. */
    public <P1, P2, P3, P4, P5, P6, P7, P8, R> WiggleFlow<R> combine(FlowFn8<P1, P2, P3, P4, P5, P6, P7, P8, R> combine, RetryPolicy retry, String queue) {
        return merge(StepNames.of(combine), retry, queue);
    }

    /** With both, queue first. */
    public <P1, P2, P3, P4, P5, P6, P7, P8, R> WiggleFlow<R> combine(FlowFn8<P1, P2, P3, P4, P5, P6, P7, P8, R> combine, String queue, RetryPolicy retry) {
        return merge(StepNames.of(combine), retry, queue);
    }

    /** A merge handler of 9 parameters, found by type in any order. */
    public <P1, P2, P3, P4, P5, P6, P7, P8, P9, R> WiggleFlow<R> combine(FlowFn9<P1, P2, P3, P4, P5, P6, P7, P8, P9, R> combine) {
        return merge(StepNames.of(combine), null, null);
    }

    /** With an explicit retry policy for the combine node. */
    public <P1, P2, P3, P4, P5, P6, P7, P8, P9, R> WiggleFlow<R> combine(FlowFn9<P1, P2, P3, P4, P5, P6, P7, P8, P9, R> combine, RetryPolicy retry) {
        return merge(StepNames.of(combine), retry, null);
    }

    /** Pinned to a dedicated worker queue. */
    public <P1, P2, P3, P4, P5, P6, P7, P8, P9, R> WiggleFlow<R> combine(FlowFn9<P1, P2, P3, P4, P5, P6, P7, P8, P9, R> combine, String queue) {
        return merge(StepNames.of(combine), null, queue);
    }

    /** With both a retry policy and a dedicated queue. */
    public <P1, P2, P3, P4, P5, P6, P7, P8, P9, R> WiggleFlow<R> combine(FlowFn9<P1, P2, P3, P4, P5, P6, P7, P8, P9, R> combine, RetryPolicy retry, String queue) {
        return merge(StepNames.of(combine), retry, queue);
    }

    /** With both, queue first. */
    public <P1, P2, P3, P4, P5, P6, P7, P8, P9, R> WiggleFlow<R> combine(FlowFn9<P1, P2, P3, P4, P5, P6, P7, P8, P9, R> combine, String queue, RetryPolicy retry) {
        return merge(StepNames.of(combine), retry, queue);
    }

    /** A merge handler of 10 parameters, found by type in any order. */
    public <P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, R> WiggleFlow<R> combine(FlowFn10<P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, R> combine) {
        return merge(StepNames.of(combine), null, null);
    }

    /** With an explicit retry policy for the combine node. */
    public <P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, R> WiggleFlow<R> combine(FlowFn10<P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, R> combine, RetryPolicy retry) {
        return merge(StepNames.of(combine), retry, null);
    }

    /** Pinned to a dedicated worker queue. */
    public <P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, R> WiggleFlow<R> combine(FlowFn10<P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, R> combine, String queue) {
        return merge(StepNames.of(combine), null, queue);
    }

    /** With both a retry policy and a dedicated queue. */
    public <P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, R> WiggleFlow<R> combine(FlowFn10<P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, R> combine, RetryPolicy retry, String queue) {
        return merge(StepNames.of(combine), retry, queue);
    }

    /** With both, queue first. */
    public <P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, R> WiggleFlow<R> combine(FlowFn10<P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, R> combine, String queue, RetryPolicy retry) {
        return merge(StepNames.of(combine), retry, queue);
    }

    /** A merge handler of 11 parameters, found by type in any order. */
    public <P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, R> WiggleFlow<R> combine(FlowFn11<P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, R> combine) {
        return merge(StepNames.of(combine), null, null);
    }

    /** With an explicit retry policy for the combine node. */
    public <P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, R> WiggleFlow<R> combine(FlowFn11<P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, R> combine, RetryPolicy retry) {
        return merge(StepNames.of(combine), retry, null);
    }

    /** Pinned to a dedicated worker queue. */
    public <P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, R> WiggleFlow<R> combine(FlowFn11<P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, R> combine, String queue) {
        return merge(StepNames.of(combine), null, queue);
    }

    /** With both a retry policy and a dedicated queue. */
    public <P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, R> WiggleFlow<R> combine(FlowFn11<P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, R> combine, RetryPolicy retry, String queue) {
        return merge(StepNames.of(combine), retry, queue);
    }

    /** With both, queue first. */
    public <P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, R> WiggleFlow<R> combine(FlowFn11<P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, R> combine, String queue, RetryPolicy retry) {
        return merge(StepNames.of(combine), retry, queue);
    }

    /** The merge named explicitly rather than referenced. */
    public <R> WiggleFlow<R> combine(String name, Class<R> result) {
        return merge(name, null, null);
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

    /** {@link #combine(String, Class)} with both, queue first. */
    public <R> WiggleFlow<R> combine(String name, Class<R> result, String queue, RetryPolicy retry) {
        return merge(name, retry, queue);
    }
}
