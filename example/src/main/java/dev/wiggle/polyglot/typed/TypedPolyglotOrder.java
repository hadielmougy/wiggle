package dev.wiggle.polyglot.typed;

import dev.wiggle.client.dsl.Blueprint;
import dev.wiggle.client.dsl.Workflow;
import dev.wiggle.core.ContextCodec;

/**
 * The topology of the typed order flow, authored once. Same idea as {@code polyglot.PolyglotOrder},
 * but the context is a typed {@link Purchase} record via {@link ContextCodec#records} instead of a
 * JSON map. The steps carry no implementation here — they are bound by name, typed on the Java side
 * ({@code Worker.handle(wf, step, codec, fn)}) and as a dict on the Python side.
 */
public final class TypedPolyglotOrder {

    public static final String NAME = "typed-order";
    public static final String PAYMENTS_QUEUE = "payments";

    private TypedPolyglotOrder() {}

    public static ContextCodec<Purchase> codec() {
        return ContextCodec.records(Purchase.class);
    }

    public static Blueprint<Purchase> blueprint() {
        return Workflow.define(NAME, codec())
                .step("validate")                                    // implemented by name, elsewhere
                .gate("in-stock", p -> p.quantity() > 0)             // predicate node; a worker supplies it
                .step("charge", PAYMENTS_QUEUE)                      // routed to the payments queue (Python)
                .effect("notify")
                .build();
    }
}