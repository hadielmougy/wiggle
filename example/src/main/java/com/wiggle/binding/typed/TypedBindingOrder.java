package com.wiggle.binding.typed;

import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.core.ContextCodec;

/**
 * The topology of the typed order flow, authored once. Same idea as {@code binding.BindingOrder},
 * but the context is a typed {@link Purchase} record via {@link ContextCodec#records} instead of a
 * JSON map. The steps carry no implementation here — they are bound by name, with typed handlers
 * ({@code Worker.handle(wf, step, codec, fn)}).
 */
public final class TypedBindingOrder {

    public static final String NAME = "typed-order";
    public static final String PAYMENTS_QUEUE = "payments";

    private TypedBindingOrder() {}

    public static ContextCodec<Purchase> codec() {
        return ContextCodec.records(Purchase.class);
    }

    public static Blueprint<Purchase> blueprint() {
        return Workflow.define(NAME, codec())
                .step("validate")                                    // implemented by name, elsewhere
                .gate("in-stock", p -> p.quantity() > 0)             // predicate node; a worker supplies it
                .step("charge", PAYMENTS_QUEUE)                      // routed to the payments queue
                .effect("notify")
                .build();
    }
}