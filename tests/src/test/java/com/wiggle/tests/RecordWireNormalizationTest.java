package com.wiggle.tests;

import com.wiggle.boot.test.WiggleBootTest;
import com.wiggle.boot.test.WiggleTestNode;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.WiggleClient;
import com.wiggle.client.worker.ForFlow;
import com.wiggle.client.worker.Worker;
import com.wiggle.core.InstanceView;
import com.wiggle.core.RecordMapper;
import com.wiggle.server.WiggleServer;
import com.wiggle.server.store.Rows.Token;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Typed records fed to the client entry points that take a workflow NAME rather than a
 * FlowSpec, plus a record signal payload. These paths hand the caller's object to the wire
 * layer directly, and the wire only speaks the JSON model -- so the client must flatten
 * records first, or the caller's object cannot be written as JSON.
 */
@WiggleBootTest
@TestPropertySource(properties = {"WIGGLE_HEARTBEAT_INTERVAL_MILLIS=300"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class RecordWireNormalizationTest {

    @Autowired WiggleTestNode node;

    public record Order(String id, String status, String approver) { }

    /** The signal payload; its fields merge into the parked instance's context. */
    public record Approval(String approver) { }

    interface OrderSteps {
        Order after(Order o);
    }

    @ForFlow("rec-wire")
    static final class OrderH {
        public Order after(Order o) { return new Order(o.id(), "APPROVED", o.approver()); }
    }

    private static FlowSpec flowSpec() {
        return FlowSpec.define("rec-wire", 1, Order.class, OrderSteps.class, (f, s) -> f
                .thenAwait("approve")
                .thenApply(s::after));
    }

    private static void awaitParked(WiggleServer server) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) {
            List<Token> p = server.engine().pendingSignals(10);
            if (!p.isEmpty()) return;
            Thread.sleep(20);
        }
    }

    @Test @DisplayName("start-by-name and signal both accept a record and flatten it for the wire")
    void recordThroughNameOverloadAndSignal() throws Exception {
        FlowSpec bp = flowSpec();
        try (WiggleClient client = node.client();
             Worker w = new Worker(client, "recwire-w").registerHandler(new OrderH())) {
            client.register(bp);
            w.start();

            String id = client.start("rec-wire", new Order("o-1", "NEW", null));
            awaitParked(node.server());
            client.signal(id, "approve", new Approval("hadi"));

            InstanceView v = client.awaitCompletion(id, Duration.ofSeconds(20));
            assertEquals("COMPLETED", v.status());
            Order out = (Order) RecordMapper.fromJson(v.context(), Order.class);
            assertEquals("o-1", out.id());
            assertEquals("APPROVED", out.status());
            assertEquals("hadi", out.approver(), "signal payload record merged into the context");
        }
    }
}
