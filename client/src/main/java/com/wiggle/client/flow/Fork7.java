package com.wiggle.client.flow;

import com.wiggle.core.RetryPolicy;

/**
 * The stage a 7-armed {@link Wiggle#allOf} returns; its {@link #combine combine} is mandatory.
 * Its parameters are found by type, in any order -- see {@link Combines}.
 */
public final class Fork7<A, B, C, D, E, F, G> extends Combines {

    private final Plan.Fork fork;

    Fork7(Plan.Fork fork) {
        this.fork = fork;
    }

    @Override
    <R> WiggleFlow<R> merge(String name, RetryPolicy retry, String queue) {
        fork.combine(name, retry, queue);
        return new WiggleFlow<>(fork);
    }
}
