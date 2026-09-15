package com.wiggle.docs;

import com.wiggle.client.WiggleConnection;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.core.Tls;

/** The code on <a href="https://wiggle.sh/patterns/cells/">wiggle.sh/patterns/cells</a>. */
public final class CellsSnippet {

    static void submit(Tls.Options tls, FlowSpec orders, Object order) {
        // docs:begin connect
        try (var wiggle = WiggleConnection.coordinator("coordinator:8099", tls, "eu-west")) {
            wiggle.clientForNamespace("orders").start(orders, order);   // routed to the owning cell
        }
        // docs:end connect
    }

    private CellsSnippet() {}
}
