package com.wiggle.binding;

import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Workflow;
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
     * The graph, with placeholder handlers that only serve to register the topology. A worker never
     * uses these -- it binds its own handlers by name -- so the author can register the blueprint
     * without running any worker at all.
     */
    public static Blueprint blueprint() {
        return Workflow.define(NAME)
                .step("validate")
                .gate("in-stock")
                .step("charge", PAYMENTS_QUEUE)
                .step("ship")
                .effect("notify")
                .build();
    }
}
