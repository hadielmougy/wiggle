package com.wiggle.client.worker;

/**
 * A step that carries its own undo: {@link Activity#execute} does the work, {@link
 * Compensable#compensate} reverses it if the instance later fails.
 *
 * <p>This type is how a step is <em>declared</em> compensable. A contract interface names such a
 * step as a zero-argument factory returning this type, and
 * {@link com.wiggle.client.flow.WiggleFlow#thenApplyCompensable thenApplyCompensable} is the only
 * thing that accepts one -- so the signature is the single place an undo is said to exist, rather
 * than being asserted once in the topology and again in the handler:
 *
 * <pre>{@code
 * interface OrderSteps {
 *     CompensableActivity<Order, Payment> authorise();   // consumes an Order, produces a Payment
 *     Payment                             confirm(Payment p);
 * }
 *
 * FlowSpec orders = FlowSpec.define("orders", 1, Order.class, OrderSteps.class, (f, s) -> f
 *         .thenApplyCompensable(s::authorise)
 *         .thenApply(s::confirm));
 * }</pre>
 *
 * <p>The worker's handler implements the same factory. A factory returning a plain {@link Activity}
 * names a step with no undo; the binder still checks the two sides agree, so a {@code Compensable}
 * instance behind a non-compensable declaration is refused rather than silently never run.
 *
 * @param <A> the context type the step consumes
 * @param <B> the context type it produces
 */
public interface CompensableActivity<A, B> extends Activity<A, B>, Compensable<A, B> {
}
