package com.wiggle.docs;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.worker.Step;
import com.wiggle.core.EventView;

import java.util.List;
import java.util.Map;

/** The consumer loop quoted in {@code docs/event-log.md}. See {@link SagaSnippet} for why these live as source. */
public final class EventLogSnippet {

    static void consume(WiggleClient client) {
        // docs:begin consume
        while (true) {
            List<EventView> batch = client.pollEvents("billing", 100, 20_000, -1);
            if (batch.isEmpty()) continue;                 // the long poll expired: ask again
            for (EventView e : batch) {
                handle(e);                                 // your side of it, idempotent by instance + seq
            }
            client.ackEvents("billing", batch.getLast().seq());
        }
        // docs:end consume
    }

    record Payment(String orderId, long amount, String currency) {}

    static Map<String, Object> charge(Map<String, Object> ctx) {
        // docs:begin emit
        Step.emit("payment.captured", new Payment("o-1234", 4200, "EUR"));
        // docs:end emit
        return ctx;
    }

    static void handle(EventView event) { }

    private EventLogSnippet() {}
}
