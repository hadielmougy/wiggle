package com.wiggle.client.flow;

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
        fork.combineName = name;
        return new WiggleFlow<>(fork);
    }
}
