package com.wiggle.client.worker;

/**
 * A typed side effect: serves an {@code effect(...)} (or plain step) node whose handler never
 * changes the context — {@link #apply} returns nothing and the context flows on untouched.
 * Matched to its step by {@link #name()} exactly like {@link Activity}.
 */
@FunctionalInterface
public interface EffectActivity<C> {

    void apply(C ctx);

    default String name() {
        return getClass().getSimpleName();
    }
}
