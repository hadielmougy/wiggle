package com.wiggle.client.worker;

/**
 * The pair of snapshots a compensator is given: what the step was handed, and what it returned.
 *
 * <p>Both are needed because a step's return <em>replaces</em> the context rather than accumulating
 * into it, so the identifier an undo needs may live on either side -- the charge reference the step
 * produced, or the order id it consumed and did not carry forward. The engine captures both at the
 * moment the step completes, so the undo sees them even if a later step drops them.
 *
 * @param <A> the type the step consumed
 * @param <B> the type it produced
 */
public interface Compensation<A, B> {

    /** What the step was given. */
    A input();

    /** What it returned. */
    B result();
}
