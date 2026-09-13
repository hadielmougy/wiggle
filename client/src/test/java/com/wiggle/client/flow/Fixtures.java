package com.wiggle.client.flow;

import com.wiggle.client.worker.Handles;

import java.util.List;

/**
 * The handler object the flow tests reference. Nothing here is ever invoked by the flow API -- the
 * methods exist to be <em>named</em> by method references and to give the chain its types, which is
 * exactly how a real handler class is used at definition time.
 */
final class Fixtures {

    record Order(String id, int quantity, List<Line> lines) {}

    record Line(String sku, long cents) {}

    record Payment(String reference) {}

    record Label(String code) {}

    record Fulfilment(String state) {}

    record Shipment(String carrier) {}

    static final class OrderHandlers {

        Order validate(Order o) { return o; }

        boolean inStock(Order o) { return o.quantity() > 0; }

        Payment charge(Order o) { return new Payment(o.id()); }

        Order reserve(Order o) { return o; }

        Label label(Order o) { return new Label(o.id()); }

        Fulfilment settle(Payment payment, Label label) { return new Fulfilment("settled"); }

        Fulfilment audit(Payment payment, Label label, Shipment shipment) { return new Fulfilment("audited"); }

        void notifyCustomer(Fulfilment f) { }

        boolean isVip(Order o) { return o.quantity() > 10; }

        Order vipPath(Order o) { return o; }

        Order standardPath(Order o) { return o; }

        boolean hasMore(Order o) { return o.quantity() > 0; }

        Order drain(Order o) { return o; }

        Line price(Line line) { return line; }

        Order total(List<Line> priced) { return new Order("x", priced.size(), priced); }

        Order totalWithBase(Order base, List<Line> priced) { return base; }

        @Handles("capture-payment")
        Payment doCapture(Order o) { return new Payment(o.id()); }
    }
}