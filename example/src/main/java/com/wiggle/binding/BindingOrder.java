package com.wiggle.binding;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.flow.Wiggle;
import com.wiggle.core.Json;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The topology of a small order flow, authored once. The graph is registered on the server; the
 * step <em>implementations</em> are attached separately, by name, by whichever worker owns them
 * (see {@link BindingDemo}). Nothing here re-declares the flow -- this is the single source of
 * truth for its shape and its version.
 *
 * <p>The context is a plain JSON map. Steps sit on the default {@code binding-order} queue except
 * {@code charge}, which is routed to {@code payments} -- the queue a dedicated payments worker polls
 * once it binds {@code charge} by name.
 */
public final class BindingOrder {

    public static final String NAME = "binding-order";
    public static final String PAYMENTS_QUEUE = "payments";

    private BindingOrder() {}

    static Map<String, Object> put(Object ctx, String key, Object value) {
        Map<String, Object> next = new LinkedHashMap<>(Json.asObject(ctx));
        next.put(key, value);
        return next;
    }

    /**
     * The steps this flow is made of, declared and not implemented. That is the whole point here: the
     * author owns the shape, and nothing on this classpath runs a step -- each worker brings its own
     * implementation and binds it by name. A worker that wants the compiler's help can implement this
     * interface; one written in Go or Python obviously cannot, and does not need to.
     */
    public interface Steps {
        Map<String, Object> validate(Map<String, Object> ctx);
        boolean inStock(Map<String, Object> ctx);
        Map<String, Object> charge(Map<String, Object> ctx);
        Map<String, Object> ship(Map<String, Object> ctx);
        void notify(Map<String, Object> ctx);
    }

    /**
     * The graph. The author can register this without running any worker at all -- {@code s} is an
     * inert stand-in that only names steps.
     */
    public static FlowSpec flowSpec() {
        return Wiggle.define(NAME, Map.class, Steps.class, (f, s) -> f
                .thenApply(s::validate)
                .thenFilter(s::inStock)
                .thenApply(s::charge, PAYMENTS_QUEUE)
                .thenApply(s::ship)
                .thenAccept(s::notify));
    }
}
