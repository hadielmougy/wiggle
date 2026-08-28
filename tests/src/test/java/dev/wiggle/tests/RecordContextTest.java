package dev.wiggle.tests;

import dev.wiggle.client.dsl.Blueprint;
import dev.wiggle.client.dsl.Branch;
import dev.wiggle.client.dsl.Workflow;
import dev.wiggle.client.WiggleClient;
import dev.wiggle.client.worker.Worker;
import dev.wiggle.core.ContextCodec;
import dev.wiggle.core.Ids;
import dev.wiggle.core.InstanceView;
import dev.wiggle.server.ServerConfig;
import dev.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A typed-record context driven through a live server and worker -- gates, forks, and the
 * shallow-diff branch merge all operating on {@link ContextCodec#records} rather than JSON maps.
 * (The pure codec round-trip has its own scenario; this covers the record path end to end.)
 */
class RecordContextTest {

    /** The workflow context: immutable, evolved through withers, merged across branches. */
    public record Shipment(String id, int items, BigDecimal total, String status,
                           String label, String invoice, List<String> log) {

        Shipment withStatus(String s) { return new Shipment(id, items, total, s, label, invoice, log); }

        Shipment withLabel(String l) { return new Shipment(id, items, total, status, l, invoice, log); }

        Shipment withInvoice(String i) { return new Shipment(id, items, total, status, label, i, log); }
    }

    private static final ContextCodec<Shipment> CODEC = ContextCodec.records(Shipment.class);

    private static Blueprint<Shipment> blueprint() {
        return Workflow.define("record-shipment", CODEC)
                .step("validate", s -> s.withStatus("VALIDATED"))
                .gate("has-items", s -> s.items() > 0)
                .fork(
                        Branch.of("labelling", b -> b.step("label", s -> s.withLabel("LBL-" + s.id()))),
                        Branch.of("billing", b -> b.step("invoice", s -> s.withInvoice("INV-" + s.id()))))
                .step("dispatch", s -> s.withStatus("DISPATCHED"))
                .build();
    }

    private static ServerConfig config() {
        return new ServerConfig(0, "rec-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    @Test @DisplayName("a record context survives gate, fork merge and typed decode end to end")
    void recordRoundTripThroughEngine() throws Exception {
        Blueprint<Shipment> bp = blueprint();
        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker w = new Worker(client, "rec-" + Ids.next("x")).register(bp)) {
            w.start();
            Shipment in = new Shipment("s-1", 3, new BigDecimal("19.99"), "NEW", null, null, List.of("created"));
            InstanceView v = client.awaitCompletion(client.start(bp, in), Duration.ofSeconds(20));
            assertEquals("COMPLETED", v.status());

            Shipment out = CODEC.decode(v.context());
            assertEquals("s-1", out.id());
            assertEquals(3, out.items());
            assertEquals(new BigDecimal("19.99"), out.total(), "BigDecimal survives the trip");
            assertEquals("DISPATCHED", out.status());
            assertEquals("LBL-s-1", out.label(), "labelling branch write survived the merge");
            assertEquals("INV-s-1", out.invoice(), "billing branch write survived the merge");
            assertEquals(List.of("created"), out.log(), "untouched list field preserved");
        }
    }

    @Test @DisplayName("a false gate on a record context ends the instance as gated")
    void recordGateShortCircuits() throws Exception {
        Blueprint<Shipment> bp = blueprint();
        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker w = new Worker(client, "rec-" + Ids.next("x")).register(bp)) {
            w.start();
            Shipment in = new Shipment("s-2", 0, new BigDecimal("1.00"), "NEW", null, null, List.of());
            InstanceView v = client.awaitCompletion(client.start(bp, in), Duration.ofSeconds(20));
            assertEquals("COMPLETED", v.status());
            assertEquals("gated:has-items", v.terminationReason());
            assertEquals("VALIDATED", CODEC.decode(v.context()).status(), "stopped after validate");
        }
    }
}
