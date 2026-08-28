package dev.wiggle.polyglot.typed;

/**
 * A typed context for the polyglot flow — a plain Java record rather than a JSON map. On the wire it
 * still travels as the same JSON object ({@code {"orderId":…, "quantity":…, …}}), which is exactly
 * why a typed Java handler and an untyped Python ({@code dict}) handler can serve different steps of
 * the same instance.
 */
public record Purchase(String orderId, int quantity, String status, String paymentRef) {

    public static Purchase of(String orderId, int quantity) {
        return new Purchase(orderId, quantity, null, null);
    }

    public Purchase withStatus(String status) {
        return new Purchase(orderId, quantity, status, paymentRef);
    }

    public Purchase withPaymentRef(String paymentRef) {
        return new Purchase(orderId, quantity, status, paymentRef);
    }
}