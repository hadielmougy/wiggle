package com.wiggle.binding.typed;

import com.wiggle.client.worker.Handlers;

/**
 * The payments worker's slice of {@code typed-order}: only {@code charge}, typed as a
 * {@link Purchase}. The topology routes it to the {@code payments} queue, so a worker bound here polls
 * just that queue -- ownership of the payment step is isolated from {@link TypedFulfilmentHandlers}.
 */
@Handlers("typed-order")
public final class TypedPaymentsHandlers {

    public Purchase charge(Purchase p) {
        return p.withPaymentRef("auth-" + p.orderId());
    }
}
