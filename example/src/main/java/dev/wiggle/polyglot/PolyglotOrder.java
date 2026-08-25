package dev.wiggle.polyglot;

import dev.wiggle.client.dsl.Activity;
import dev.wiggle.client.dsl.Blueprint;
import dev.wiggle.client.dsl.Workflow;
import dev.wiggle.core.Json;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The topology of a small order flow, authored once. The graph is registered on the server; the
 * step <em>implementations</em> are attached separately, by name, from whatever language owns them
 * (see {@link PolyglotDemo} for the Java side and {@code clients/python/examples/polyglot_worker.py}
 * for the Python payments worker). Nothing here re-declares the flow in two places -- this is the
 * single source of truth for its shape and its version.
 *
 * <p>The context is a plain JSON map so Java and Python handlers exchange it verbatim. Steps sit on
 * the default {@code polyglot-order} queue except {@code charge}, which is routed to {@code payments}
 * -- the queue the Python worker will poll once it binds {@code charge} by name.
 */
public final class PolyglotOrder {

    public static final String NAME = "polyglot-order";
    public static final String PAYMENTS_QUEUE = "payments";

    private PolyglotOrder() {}

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
    public static Blueprint<Map<String, Object>> blueprint() {
        return Workflow.define(NAME)
                .step("validate")
                .gate("in-stock", ctx -> ((Number) ctx.get("quantity")).intValue() > 0)
                .step("charge", PAYMENTS_QUEUE)
                .step("ship")
                .effect("notify")
                .build();
    }
}
