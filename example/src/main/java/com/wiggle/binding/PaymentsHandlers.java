package com.wiggle.binding;

import com.wiggle.client.worker.Handlers;

import java.util.Map;

import static com.wiggle.binding.BindingOrder.put;

/**
 * The payments worker's slice of {@code binding-order}: only {@code charge}, which the topology routes
 * to the {@code payments} queue. A worker bound to this set polls just that queue, so ownership of the
 * payment step is isolated from {@link FulfilmentHandlers}.
 */
@Handlers("binding-order")
public final class PaymentsHandlers {

    public Map<String, Object> charge(Map<String, Object> ctx) {
        return put(ctx, "paymentRef", "auth-" + ctx.get("orderId"));
    }
}
