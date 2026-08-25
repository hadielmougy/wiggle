package dev.wiggle.order;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * The workflow context. A record: immutable, trivially serialisable, and every step
 * returns a new one, which is what makes replay after a crash well behaved.
 *
 * <p>{@code currency} was added in schema v2; instances created under v1 are migrated
 * forward by the {@code upcast} in {@link OrderFulfilment}, which defaults it. See the
 * "Evolving the context schema" section of the README.
 */
public record Order(String orderId, String customer, int quantity, BigDecimal amount, String currency,
                    String status, String paymentRef, String shipmentRef, String trackingLabel,
                    List<String> audit) {

    public static Order of(String orderId, String customer, int quantity, BigDecimal amount) {
        return new Order(orderId, customer, quantity, amount, "USD", "NEW", null, null, null, List.of());
    }

    public Order withStatus(String s) {
        return new Order(orderId, customer, quantity, amount, currency, s, paymentRef, shipmentRef, trackingLabel, audit);
    }

    public Order withPaymentRef(String ref) {
        return new Order(orderId, customer, quantity, amount, currency, status, ref, shipmentRef, trackingLabel, audit);
    }

    public Order withShipmentRef(String ref) {
        return new Order(orderId, customer, quantity, amount, currency, status, paymentRef, ref, trackingLabel, audit);
    }

    public Order withTrackingLabel(String label) {
        return new Order(orderId, customer, quantity, amount, currency, status, paymentRef, shipmentRef, label, audit);
    }

    public Order log(String entry) {
        List<String> next = new ArrayList<>(audit);
        next.add(entry);
        return new Order(orderId, customer, quantity, amount, currency, status, paymentRef, shipmentRef,
                trackingLabel, List.copyOf(next));
    }
}
