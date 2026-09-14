package com.wiggle.client.flow;

import com.wiggle.core.RetryPolicy;

/**
 * The stage an {@link Wiggle#allOf} of more than ten arms returns. Past ten the combine's parameter
 * list outruns the typed {@code FlowFnN} series, so the merge is named rather than referenced; the
 * handler is the usual one -- a parameter per arm, in fork order.
 */
public final class ForkN {

    private final Plan.Fork fork;

    ForkN(Plan.Fork fork) {
        this.fork = fork;
    }

    /** The merge for the preceding fan-out, named rather than referenced. */
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

    private <R> WiggleFlow<R> merge(String name, RetryPolicy retry, String queue) {
        fork.combine(name, retry, queue);
        return new WiggleFlow<>(fork);
    }
}
