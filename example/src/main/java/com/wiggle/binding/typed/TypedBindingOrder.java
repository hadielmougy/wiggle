package com.wiggle.binding.typed;

import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Workflow;

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

    public static Blueprint blueprint() {
        return Workflow.define(NAME)
                .step("validate")                                    // implemented by name, elsewhere
                .gate("in-stock")                                    // predicate node; a worker supplies it
                .step("charge", PAYMENTS_QUEUE)                      // routed to the payments queue
                .effect("notify")
                .build();
    }
}