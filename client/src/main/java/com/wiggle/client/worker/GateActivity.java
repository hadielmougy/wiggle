package com.wiggle.client.worker;

/**
 * A typed gate: serves a {@code gate(...)} node. {@link #test} returns whether the flow proceeds —
 * {@code false} ends the instance cleanly (a normal outcome, not an error). Matched to its step by
 * {@link #name()} exactly like {@link Activity}.
 */
@FunctionalInterface
public interface GateActivity<C> {

    boolean test(C ctx);

    default String name() {
        return getClass().getSimpleName();
    }
}
