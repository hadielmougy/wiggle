package com.wiggle.order;


/**
 * The steps of {@code order-fulfilment}, as a contract: every step's name and its signature, with no
 * implementation. {@link OrderFulfilment} names the topology through this, and {@link OrderHandlers}
 * implements it on the worker.
 *
 * <p>The interface is what keeps the two honest. The spec only ever <em>names</em> steps -- the code
 * that runs them is bound by name on a worker -- so naming them here says exactly that, and an
 * implementation that declares {@code implements OrderSteps} is checked by the compiler to have every
 * one of them, with the right types. Neither half can drift without the build noticing.
 *
 * <p>Implementing it is not required, though: binding is by name, so a worker may serve these steps
 * with any {@code @ForFlow} object whose methods happen to match. That is what lets a step be served
 * by a different service, in a different language -- the interface is the convenience, the name is
 * the contract.
 */
public interface OrderSteps {

    Order validate(Order order);

    boolean inStock(Order order);

    Order authorise(Order order);

    Order capture(Order order);

    Order reserveStock(Order order);

    Order printLabel(Order order);

    /**
     * The combine. Its parameters are found by type: both arms produce an {@code Order}, so they take
     * the last two {@code Order} parameters in fork order, and the first is the pre-fork order.
     */
    Order merge(Order payment, Order shipping);

    Order notify(Order order);

    void audit(Order order);
}
