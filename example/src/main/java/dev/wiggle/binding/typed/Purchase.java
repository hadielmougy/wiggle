package dev.wiggle.binding.typed;

/**
 * A typed context for the flow — a plain Java record rather than a JSON map. On the wire it still
 * travels as the same JSON object ({@code {"orderId":…, "quantity":…, …}}), so a typed handler and an
 * untyped (map) handler can serve different steps of the same instance interchangeably.
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