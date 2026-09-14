package com.wiggle.order;

import com.wiggle.client.worker.Context;

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
 * with any {@code @Handlers} object whose methods happen to match. That is what lets a step be served
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
     * The combine: the pre-fork order, then one parameter per fork arm, in fork order. {@code @Context}
     * is part of the shape, so it is declared here as well as on the implementation -- Java does not
     * inherit parameter annotations, and each side is read by a different half of the system (this one
     * when the spec is defined, the implementation's when a worker binds).
     */
    Order merge(@Context Order base, Order payment, Order shipping);

    Order notify(Order order);

    void audit(Order order);
}
