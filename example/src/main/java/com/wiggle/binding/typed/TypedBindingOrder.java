package com.wiggle.binding.typed;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.flow.Wiggle;

/**
 * The topology of the typed order flow, authored once. Same idea as {@code binding.BindingOrder},
 * but the context is a typed {@link Purchase} record instead of a JSON map. The steps carry no
 * implementation here — they are bound by name, with typed {@code @Handlers} methods
 * ({@code Purchase -> Purchase}); see {@link TypedFulfilmentHandlers} and {@link TypedPaymentsHandlers}.
 */
public final class TypedBindingOrder {

    public static final String NAME = "typed-order";
    public static final String PAYMENTS_QUEUE = "payments";

    private TypedBindingOrder() {}

    public static FlowSpec flowSpec() {
        return Wiggle.define(NAME, Purchase.class, f -> f
                .thenApply("validate")                               // implemented by name, elsewhere
                .thenFilter("in-stock")                              // predicate node; a worker supplies it
                .thenApply("charge").onQueue(PAYMENTS_QUEUE)         // routed to the payments queue
                .thenAccept("notify"));
    }
}