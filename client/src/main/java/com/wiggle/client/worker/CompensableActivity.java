package com.wiggle.client.worker;

/**
 * A step that carries its own undo: {@link Activity#execute} does the work, {@link
 * Compensable#compensate} reverses it if the instance later fails.
 *
 * <p>This type is how a step is <em>declared</em> compensable. A contract interface names such a
 * step as a zero-argument factory returning this type, and
 * {@link com.wiggle.client.flow.WiggleFlow#thenActivity} reads that declaration to mark the node --
 * so the signature is the single place it is said, rather than being asserted once in the topology
 * and again in the handler:
 *
 * <pre>{@code
 * interface OrderSteps {
 *     CompensableActivity<Order> reserve();     // has an undo
 *     Order                      confirm(Order o);
 * }
 *
 * FlowSpec orders = FlowSpec.define("orders", Order.class, OrderSteps.class, (f, s) -> f
 *         .thenActivity(s::reserve)
 *         .thenApply(s::confirm));
 * }</pre>
 *
 * <p>The worker's handler implements the same factory, returning an instance that does both halves.
 * A factory that returns a plain {@link Activity} names a step with no undo; the binder still checks
 * the two sides agree, so a {@code Compensable} instance behind a non-compensable declaration is
 * refused rather than silently never run.
 *
 * @param <C> the context type the step consumes and produces
 */
public interface CompensableActivity<C> extends Activity<C>, Compensable<C> {
}
