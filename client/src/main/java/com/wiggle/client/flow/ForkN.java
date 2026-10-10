package com.wiggle.client.flow;

import com.wiggle.core.RetryPolicy;

/**
 * The stage an {@link Wiggle#allOf} of more than ten arms returns; its {@link #combine combine} is
 * mandatory. Its parameters are found by type, in any order -- see {@link Combines}.
 */
public final class ForkN extends Combines {

    private final Plan.Fork fork;

    ForkN(Plan.Fork fork) {
        this.fork = fork;
    }

    @Override
    <R> WiggleFlow<R> merge(String name, RetryPolicy retry, String queue) {
        fork.combine(name, retry, queue);
        return new WiggleFlow<>(fork);
    }
}
