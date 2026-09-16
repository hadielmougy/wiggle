package com.wiggle.docs;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.CompensableActivity;

/**
 * The code published on <a href="https://wiggle.sh/patterns/saga/">wiggle.sh/patterns/saga</a>,
 * as source the compiler checks.
 *
 * <p>The regions below are the source of truth: this class is compiled by the ordinary build, and
 * {@code sync-docs.sh} rewrites the page's fenced blocks from them. Break the API and the build
 * fails here; edit the page's Java by hand and the next sync overwrites it. {@code SagaSnippetTest}
 * checks the marker comments stay well formed, since a silent extraction failure publishes nothing.
 *
 * <p>Its companion is {@link BookingHandlers}, which the same page shows under "The handlers".
 */
public final class SagaSnippet {

    public record Booking(String reservationRef, String paymentRef, String tracking) {
        Booking withReservationRef(String r) { return new Booking(r, paymentRef, tracking); }
        Booking withPaymentRef(String p) { return new Booking(reservationRef, p, tracking); }
        Booking withTracking(String t) { return new Booking(reservationRef, paymentRef, t); }
    }

    // docs:begin contract
    interface BookingSteps {
        CompensableActivity<Booking, Booking> reserveStock();   // has an undo
        CompensableActivity<Booking, Booking> chargeCard();     // has an undo
        Booking bookCourier(Booking b);                         // no undo: nothing external to unwind
    }
    // docs:end contract

    static FlowSpec define() {
        // docs:begin topology
        FlowSpec booking = FlowSpec.define("booking", Booking.class, BookingSteps.class, (f, s) -> f
                .thenApplyCompensable(s::reserveStock)
                .thenApplyCompensable(s::chargeCard)
                .thenApply(s::bookCourier));
        // docs:end topology
        return booking;
    }

    private SagaSnippet() {}
}
