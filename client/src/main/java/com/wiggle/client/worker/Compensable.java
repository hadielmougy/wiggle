package com.wiggle.client.worker;

/**
 * An activity that knows how to undo itself. Implemented alongside {@link Activity#execute} on the
 * same object -- the do and the undo travel together, which is the point:
 *
 * <pre>{@code
 * class CapturePayment implements CompensableActivity<Order, Payment> {
 *     public Payment execute(Order o)                    { return gateway.charge(o); }
 *     public void compensate(Compensation<Order, Payment> c) { gateway.refund(c.result().reference()); }
 * }
 * }</pre>
 *
 * <p>The undo runs as a real durable task, claimed and retried like any other, not as a callback in
 * the failing step's thread.
 *
 * <p>The topology remains the contract: a step is compensated on failure only if the workflow
 * declares it, by naming the step through a {@link CompensableActivity} factory. The binder verifies
 * the pairing both ways at bind time — a declared undo without a {@code Compensable} handler, or a
 * {@code Compensable} handler on an undeclared step, refuses to bind.
 *
 * @param <A> the type the step consumed
 * @param <B> the type it produced
 */
public interface Compensable<A, B> {

    /** Undoes this step's external effect, given its input/result snapshots. */
    void compensate(Compensation<A, B> comp);
}
