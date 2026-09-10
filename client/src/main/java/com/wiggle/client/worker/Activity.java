package com.wiggle.client.worker;

/**
 * A typed, single-step task handler: {@link #execute} receives the context decoded into {@code C}
 * and returns the next context — sent whole, it REPLACES the previous value server-side (a
 * {@code null} return leaves it untouched).
 *
 * <p>Registered via a <b>factory method</b> on a {@link Handlers @Handlers} class: a
 * zero-parameter method returning an activity type is invoked once at registration and its result
 * serves the step named by the method (or by {@link Handles @Handles}) — so the workflow's whole
 * implementation still arrives through one {@code worker.handlers(...)} call:
 *
 * <pre>{@code
 * @Handlers("order-fulfilment")
 * class OrderHandlers {
 *     public Order validate(Order o) { ... }              // plain method handler
 *
 *     public Activity<Order> capturePayment() {           // factory -> serves "capture-payment"
 *         return new CapturePayment(gateway);             // may implement Compensable
 *     }
 * }
 * }</pre>
 *
 * <p>Implement {@link Compensable} as well to pair the step with its undo — the class then carries
 * both sides of the saga. Inside a {@code forEach} body the parameter is the element; the frozen
 * pre-loop context is available via {@link Step#base()}.
 */
@FunctionalInterface
public interface Activity<C> {

    /** Runs the step; the return is the complete next context (null = unchanged). */
    C execute(C ctx);
}
