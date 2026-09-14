package com.wiggle.client.flow;

import java.io.Serializable;
import java.util.Map;

/**
 * {@link FlowItems} for a map-valued collection: the engine fans out over the map's values and the
 * combine receives a {@code Map} keyed the way the input was, so each result can be traced back to
 * the key it came from.
 *
 * <p>Same rules as {@link FlowItems} -- a reference to the context's own accessor
 * ({@code Basket::itemsBySku}), read for its name, never invoked.
 *
 * @param <A> the context type the map is read from
 * @param <K> the map's key type -- preserved through the combine
 * @param <V> the value type -- each value is one branch's whole context
 */
@FunctionalInterface
public interface FlowItemsMap<A, K, V> extends Serializable {

    Map<K, V> apply(A ctx);
}
