package com.wiggle.client.worker;

/**
 * A step implemented as an object rather than a method, for when the step needs dependencies or
 * carries an undo. A handler class supplies one from a zero-argument factory method, and the factory's
 * name is the step it serves:
 *
 * <pre>{@code
 * @ForFlow("order-fulfilment")
 * class OrderHandlers {
 *     public Order validate(Order o) { ... }                   // plain method handler
 *
 *     public Activity<Order, Payment> capturePayment() {       // factory -> serves "capturePayment"
 *         return new CapturePayment(gateway);                  // may implement Compensable
 *     }
 * }
 * }</pre>
 *
 * <p>Like a plain method handler, an activity maps one context type to another: {@code execute}
 * takes the step's input and returns the complete next context, which may be a different type. A
 * step that carries an undo declares {@link CompensableActivity} instead, and the topology names it
 * through {@link com.wiggle.client.flow.WiggleFlow#thenApplyCompensable thenApplyCompensable}.
 *
 * <p>Inside a {@code forEach} body the input is the element; the frozen pre-loop context is
 * available via {@link Step#base()}.
 *
 * @param <A> the context type the step consumes
 * @param <B> the context type it produces
 */
@FunctionalInterface
public interface Activity<A, B> {

    /** Runs the step; the return is the complete next context (null = unchanged). */
    B execute(A ctx);
}
