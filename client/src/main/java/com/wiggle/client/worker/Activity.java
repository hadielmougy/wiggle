package com.wiggle.client.worker;

/**
 * A typed, single-step handler: one class per step, the alternative to a {@link Handlers @Handlers}
 * class of methods. {@link #execute} receives the context decoded into {@code C} and returns the
 * next context — sent whole, it REPLACES the previous value server-side (a {@code null} return
 * leaves it untouched). Register with {@link Worker#activities}.
 *
 * <p>The step it serves is matched by {@link #name()} — default: the class's simple name, under the
 * same case/style-insensitive folding as method matching, so {@code class CapturePayment} serves the
 * step {@code capture-payment} with no override. Override {@code name()} (or register with an
 * explicit name) when they must differ.
 *
 * <p>Implement {@link Compensable} as well to pair the step with its undo — the class then carries
 * both sides of the saga, checked at bind time. Inside a {@code forEach} body the parameter is the
 * element; the frozen pre-loop context is available via {@link Step#base()}.
 *
 * <p>Both registration styles coexist on one worker: typed activities for steps that carry
 * capabilities (compensation) or deserve their own class; a {@code @Handlers} class for concise
 * flows and for combine methods (whose {@link Arm @Arm}/collection signatures have no typed
 * equivalent yet).
 */
@FunctionalInterface
public interface Activity<C> {

    /** Runs the step; the return is the complete next context (null = unchanged). */
    C execute(C ctx);

    /** The step this activity serves, matched case/style-insensitively. Default: the class name. */
    default String name() {
        return getClass().getSimpleName();
    }
}
