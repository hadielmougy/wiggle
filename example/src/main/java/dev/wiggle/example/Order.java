package dev.wiggle.example;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * The workflow context. A record: immutable, trivially serialisable, and every step
 * returns a new one, which is what makes replay after a crash well behaved.
 */
public record Order(String orderId, String customer, int quantity, BigDecimal amount,
                    String status, String paymentRef, String shipmentRef, String trackingLabel,
                    List<String> audit) {

    public static Order of(String orderId, String customer, int quantity, BigDecimal amount) {
        return new Order(orderId, customer, quantity, amount, "NEW", null, null, null, List.of());
    }

    public Order withStatus(String s) {
        return new Order(orderId, customer, quantity, amount, s, paymentRef, shipmentRef, trackingLabel, audit);
    }

    public Order withPaymentRef(String ref) {
        return new Order(orderId, customer, quantity, amount, status, ref, shipmentRef, trackingLabel, audit);
    }

    public Order withShipmentRef(String ref) {
        return new Order(orderId, customer, quantity, amount, status, paymentRef, ref, trackingLabel, audit);
    }

    public Order withTrackingLabel(String label) {
        return new Order(orderId, customer, quantity, amount, status, paymentRef, shipmentRef, label, audit);
    }

    public Order log(String entry) {
        List<String> next = new ArrayList<>(audit);
        next.add(entry);
        return new Order(orderId, customer, quantity, amount, status, paymentRef, shipmentRef,
                trackingLabel, List.copyOf(next));
    }
}
