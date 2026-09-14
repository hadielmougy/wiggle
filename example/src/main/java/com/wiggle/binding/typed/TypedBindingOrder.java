package com.wiggle.binding.typed;

import com.wiggle.client.flow.FlowSpec;

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

    /** The steps, declared and not implemented -- each worker brings its own and binds by name. */
    public interface Steps {
        Purchase validate(Purchase p);
        boolean inStock(Purchase p);
        Purchase charge(Purchase p);
        void notify(Purchase p);
    }

    public static FlowSpec flowSpec() {
        return FlowSpec.define(NAME, Purchase.class, Steps.class, (f, s) -> f
                .thenApply(s::validate)                              // implemented by name, elsewhere
                .thenFilter(s::inStock)                              // predicate node; a worker supplies it
                .thenApply(s::charge, PAYMENTS_QUEUE)                // routed to the payments queue
                .thenAccept(s::notify));
    }
}