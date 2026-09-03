package com.wiggle.binding;

import com.wiggle.client.worker.Handlers;

import java.util.Map;

import static com.wiggle.binding.BindingOrder.put;

/**
 * The fulfilment worker's slice of {@code binding-order}: every step but {@code charge}, bound by
 * name. The context is a plain JSON map, so each method takes a {@code Map<String,Object>}. Because
 * this set has no {@code charge} method, a worker bound to it never serves (nor polls) the payments
 * queue -- ownership of that step is left to {@link PaymentsHandlers}.
 */
@Handlers("binding-order")
public final class FulfilmentHandlers {

    public Map<String, Object> validate(Map<String, Object> ctx) {
        return put(ctx, "status", "VALIDATED");
    }

    public boolean inStock(Map<String, Object> ctx) {
        return ((Number) ctx.get("quantity")).intValue() > 0;
    }

    public Map<String, Object> ship(Map<String, Object> ctx) {
        return put(ctx, "trackingLabel", "DHL-" + ctx.get("orderId"));
    }

    public void notify(Map<String, Object> ctx) {
        System.out.println("   [fulfilment] notified " + ctx.get("orderId"));
    }
}
