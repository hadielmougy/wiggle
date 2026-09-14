package com.wiggle.client.flow;

import com.wiggle.client.worker.Context;
import com.wiggle.client.worker.Handles;

import java.util.List;

/**
 * The step contract the flow tests name their workflows through, and the records those steps move
 * between. There is deliberately no implementation anywhere in this file: a spec only ever names its
 * steps, so an interface is all it needs, and these tests never run one.
 */
final class Fixtures {

    record Order(String id, int quantity, List<Line> lines) {}

    record Line(String sku, long cents) {}

    record Payment(String reference) {}

    record Label(String code) {}

    record Fulfilment(String state) {}

    record Shipment(String carrier) {}

    interface Steps {

        Order validate(Order o);

        boolean inStock(Order o);

        Payment charge(Order o);

        Order reserve(Order o);

        Label label(Order o);

        Fulfilment settle(Payment payment, Label label);

        Fulfilment settleWithBase(@Context Order base, Payment payment,
                                  Label label);

        Fulfilment audit(Payment payment, Label label,
                         Shipment shipment);

        /** The arms bind by position, in fork order. */
        Fulfilment settlePositionally(Payment payment, Label label);

        /** Right shape for a context-taking combine, but the context parameter is not annotated. */
        Fulfilment unannotatedBase(Order base, Payment payment,
                                   Label label);

        void notifyCustomer(Fulfilment f);

        boolean isVip(Order o);

        Order vipPath(Order o);

        Order standardPath(Order o);

        boolean hasMore(Order o);

        Order drain(Order o);

        Line price(Line line);

        Order total(List<Line> priced);

        Order totalWithBase(Order base, List<Line> priced);

        /** A five-armed combine, to exercise the wider end of the typed series. */
        Fulfilment settleFive(@Context Order base, Payment a, Label b, Shipment c, Order d, Line e);

        Payment armA(Order o);

        Label armB(Order o);

        Shipment armC(Order o);

        Order armD(Order o);

        Line armE(Order o);

        @Handles("capture-payment")
        Payment doCapture(Order o);
    }
}