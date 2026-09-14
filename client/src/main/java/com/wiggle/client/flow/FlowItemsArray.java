package com.wiggle.client.flow;

import java.io.Serializable;

/**
 * {@link FlowItems} for an array-valued collection. An array is a JSON array once persisted, so the
 * engine treats it exactly as it treats a list; this exists so an accessor declared {@code Item[]}
 * type-checks without being wrapped.
 *
 * <p>Same rules as {@link FlowItems} -- a reference to the context's own accessor
 * ({@code Basket::skus}), read for its name, never invoked.
 *
 * @param <A> the context type the array is read from
 * @param <E> the element type -- each element is one branch's whole context
 */
@FunctionalInterface
public interface FlowItemsArray<A, E> extends Serializable {

    E[] apply(A ctx);
}
