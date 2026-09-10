package com.wiggle.client.worker;

/**
 * The undo half of a saga step: an {@link Activity} (or {@link EffectActivity}) that also
 * implements {@code Compensable} carries its own compensation, so the code that does the thing and
 * the code that undoes it live in one class and the pairing is checked by the compiler.
 *
 * <p>{@link #compensate} receives a {@link Compensation} carrying <b>both snapshots of this
 * step</b> — {@code result()} (the context as the step left it) and {@code input()} (as it
 * received it) — captured at step completion, not read from the instance's latest context. Under
 * replace semantics a later step may have dropped the very fields the undo needs; the snapshots
 * guarantee they are present, and the input side means undo-only data (idempotency keys, previous
 * values to restore) never has to pollute the business context. See
 * {@code docs/saga-compensation.md}.
 *
 * <p>Compensators are at-least-once, like every handler — make them idempotent (refund by an
 * idempotency key, not blindly).
 *
 * <p>The topology remains the contract: a step is compensated on failure only if the workflow
 * declares it ({@code .compensate(...)}). At bind time the pairing is verified both ways once the
 * engine's compensation phase lands; until then the binder records the compensator on the binding.
 */
public interface Compensable<C> {

    /** Undoes this step's external effect, given its input/result snapshots. */
    void compensate(Compensation<C> comp);
}
