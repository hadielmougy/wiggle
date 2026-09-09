package com.wiggle.client.worker;

/**
 * The undo half of a saga step: an {@link Activity} (or {@link EffectActivity}) that also
 * implements {@code Compensable} carries its own compensation, so the code that does the thing and
 * the code that undoes it live in one class and the pairing is checked by the compiler.
 *
 * <p>{@link #compensate} receives the context <b>as this step's forward execution left it</b> — a
 * snapshot taken at step completion, not the instance's latest context. Under replace semantics a
 * later step may have dropped the very fields the undo needs (a {@code paymentRef}); the snapshot
 * guarantees they are present. See {@code docs/saga-compensation.md}.
 *
 * <p>Compensators are at-least-once, like every handler — make them idempotent (refund by an
 * idempotency key, not blindly).
 *
 * <p>The topology remains the contract: a step is compensated on failure only if the workflow
 * declares it ({@code .compensate(...)}). At bind time the pairing is verified both ways — a
 * declared-compensable step bound to a non-{@code Compensable} activity fails fast, and vice
 * versa — once the engine's compensation phase lands; until then the binder records the
 * compensator on the binding for that wiring.
 */
public interface Compensable<C> {

    /** Undoes this step's external effect, given the post-step context snapshot. */
    void compensate(C snapshot);
}
