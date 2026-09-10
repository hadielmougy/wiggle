package com.wiggle.client.worker;

/**
 * A typed gate: serves a {@code gate(...)} node. {@link #test} returns whether the flow proceeds —
 * {@code false} ends the instance cleanly (a normal outcome, not an error). Registered via a
 * factory method on a {@link Handlers @Handlers} class, exactly like {@link Activity}.
 */
@FunctionalInterface
public interface GateActivity<C> {

    boolean test(C ctx);
}
