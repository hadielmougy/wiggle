package com.wiggle.docs;

import com.wiggle.docs.SagaSnippet.BookingSteps;
import com.wiggle.client.worker.CompensableActivity;
import com.wiggle.client.worker.Compensation;
import com.wiggle.client.worker.ForFlow;
import com.wiggle.docs.SagaSnippet.Booking;

/**
 * The handler half of the code published on
 * <a href="https://wiggle.sh/patterns/saga/">wiggle.sh/patterns/saga</a> -- see {@link SagaSnippet}
 * for why the page draws its Java from compiled source instead of holding its own copy.
 *
 * <p>Top-level rather than nested inside {@code SagaSnippet} for one reason: a nested class carries
 * {@code static}, and the page would publish {@code static class BookingHandlers} -- an artifact of
 * how the fixture is stored, not something a reader should write. What the page shows should be what
 * you would type.
 *
 * <p>The three service fields are inside {@code docs:skip} for the same reason: they exist so this
 * compiles, and how a booking service gets a payment gateway is not what the page is teaching.
 */
// docs:begin handlers
@ForFlow("booking")
class BookingHandlers implements BookingSteps {
    // docs:skip
    private Wms wms;
    private Gateway gateway;
    private Courier courier;

    interface Wms { String reserve(Booking b); void release(String ref); }
    interface Gateway { String capture(Booking b); void refund(String ref, String key); }
    interface Courier { String book(Booking b); }

    private static String idempotencyKey(Booking b) { return b.reservationRef(); }
    // docs:resume

    public CompensableActivity<Booking, Booking> reserveStock() {
        return new CompensableActivity<>() {
            public Booking execute(Booking b) {
                return b.withReservationRef(wms.reserve(b));
            }
            public void compensate(Compensation<Booking, Booking> c) {
                wms.release(c.result().reservationRef());   // the step's OWN result snapshot
            }
        };
    }

    public CompensableActivity<Booking, Booking> chargeCard() {
        return new CompensableActivity<>() {
            public Booking execute(Booking b) {
                return b.withPaymentRef(gateway.capture(b));
            }
            public void compensate(Compensation<Booking, Booking> c) {
                gateway.refund(c.result().paymentRef(),
                               idempotencyKey(c.input()));  // undo-only data from the INPUT snapshot
            }
        };
    }

    public Booking bookCourier(Booking b) { return b.withTracking(courier.book(b)); }
}
// docs:end handlers
