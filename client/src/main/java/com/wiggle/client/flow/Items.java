package com.wiggle.client.flow;

import com.wiggle.core.RetryPolicy;

import java.util.function.UnaryOperator;

/**
 * The stage {@link WiggleFlow#thenForEach} returns; its combine is mandatory. Each item ran on its own
 * isolated context -- the element itself -- so a combine is the only way the results reach the flow.
 */
public final class Items {

    private final WiggleFlow<?> from;
    private final String name;
    private final String itemsKey;
    private final UnaryOperator<GraphBuilder> body;

    Items(WiggleFlow<?> from, String name, String itemsKey, UnaryOperator<GraphBuilder> body) {
        this.from = from;
        this.name = name;
        this.itemsKey = itemsKey;
        this.body = body;
    }

    /**
     * The merge for the preceding forEach: a handler taking the collected item results -- a
     * {@code List} ordered by item index when the input was a list, a {@code Map} keyed like the input
     * when it was a map -- and returning the complete post-join context.
     */
    public <X, R> WiggleFlow<R> combine(FlowFn<X, R> combine) {
        return merge(StepNames.of(combine), null, null);
    }

    /**
     * {@link #combine(FlowFn)} for a handler that also takes the pre-forEach context: its
     * {@link com.wiggle.client.worker.Context @Context} parameter plus the collected results.
     */
    public <C, X, R> WiggleFlow<R> combine(FlowFn2<C, X, R> combine) {
        return merge(StepNames.of(combine), null, null);
    }

    /** The merge named explicitly. */
    public <R> WiggleFlow<R> combine(String name, Class<R> result) {
        return merge(name, null, null);
    }

    /** {@link #combine(FlowFn<X,)} with an explicit retry policy for the combine node. */
    public <X, R> WiggleFlow<R> combine(FlowFn<X, R> combine, RetryPolicy retry) {
        return merge(StepNames.of(combine), retry, null);
    }

    /** {@link #combine(FlowFn<X,)} pinned to a dedicated worker queue. */
    public <X, R> WiggleFlow<R> combine(FlowFn<X, R> combine, String queue) {
        return merge(StepNames.of(combine), null, queue);
    }

    /** {@link #combine(FlowFn<X,)} with both a retry policy and a dedicated queue. */
    public <X, R> WiggleFlow<R> combine(FlowFn<X, R> combine, RetryPolicy retry, String queue) {
        return merge(StepNames.of(combine), retry, queue);
    }

    /** {@link #combine(FlowFn<X,)} with both, queue first. */
    public <X, R> WiggleFlow<R> combine(FlowFn<X, R> combine, String queue, RetryPolicy retry) {
        return merge(StepNames.of(combine), retry, queue);
    }

    /** {@link #combine(FlowFn2<C,)} with an explicit retry policy for the combine node. */
    public <C, X, R> WiggleFlow<R> combine(FlowFn2<C, X, R> combine, RetryPolicy retry) {
        return merge(StepNames.of(combine), retry, null);
    }

    /** {@link #combine(FlowFn2<C,)} pinned to a dedicated worker queue. */
    public <C, X, R> WiggleFlow<R> combine(FlowFn2<C, X, R> combine, String queue) {
        return merge(StepNames.of(combine), null, queue);
    }

    /** {@link #combine(FlowFn2<C,)} with both a retry policy and a dedicated queue. */
    public <C, X, R> WiggleFlow<R> combine(FlowFn2<C, X, R> combine, RetryPolicy retry, String queue) {
        return merge(StepNames.of(combine), retry, queue);
    }

    /** {@link #combine(FlowFn2<C,)} with both, queue first. */
    public <C, X, R> WiggleFlow<R> combine(FlowFn2<C, X, R> combine, String queue, RetryPolicy retry) {
        return merge(StepNames.of(combine), retry, queue);
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

    private <R> WiggleFlow<R> merge(String combineName, RetryPolicy retry, String queue) {
        return from.recordForEach(name, itemsKey, body, combineName, retry, queue);
    }
}
