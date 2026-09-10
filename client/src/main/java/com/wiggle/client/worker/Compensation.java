package com.wiggle.client.worker;

/**
 * What a {@link Compensable} compensator receives: both context snapshots of the step it undoes,
 * behind named accessors — never two same-typed positional parameters, so before/after can't be
 * transposed, and the one signature implementors depend on never has to change (future slots —
 * the failure reason, the attempt, the instance id — arrive as new accessors, non-breaking).
 *
 * <ul>
 *   <li>{@link #result()} — the context <b>as this step left it</b>: the step's own products
 *       ({@code paymentRef}) live here regardless of what later steps replaced. The primary
 *       snapshot for most undos.</li>
 *   <li>{@link #input()} — the context <b>as this step received it</b>: restore-previous-value
 *       undos and undo-only data (an idempotency key derived from the input) read from here, so
 *       nothing has to be smuggled through the business context just to reach the compensator.</li>
 * </ul>
 *
 * For a {@link EffectActivity} the two are the same context (effects don't change it).
 */
public interface Compensation<C> {

    /** The context as this step received it. */
    C input();

    /** The context as this step left it — the post-step snapshot. */
    C result();
}
