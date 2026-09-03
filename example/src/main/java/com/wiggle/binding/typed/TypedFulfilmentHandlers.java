package com.wiggle.binding.typed;

import com.wiggle.client.worker.Handlers;

/**
 * The fulfilment worker's slice of {@code typed-order}: every step but {@code charge}, with a typed
 * {@link Purchase} context. Each method takes and returns a {@code Purchase} (a {@code boolean} return
 * makes {@code in-stock} a gate, {@code void} makes {@code notify} an effect). With no {@code charge}
 * method, a worker bound here never serves the payments queue -- see {@link TypedPaymentsHandlers}.
 */
@Handlers("typed-order")
public final class TypedFulfilmentHandlers {

    public Purchase validate(Purchase p) {
        return p.withStatus("VALIDATED");
    }

    public boolean inStock(Purchase p) {
        return p.quantity() > 0;
    }

    public void notify(Purchase p) {
        System.out.println("   [fulfilment] notified " + p.orderId()
                + " status=" + p.status() + " payment=" + p.paymentRef());
    }
}
