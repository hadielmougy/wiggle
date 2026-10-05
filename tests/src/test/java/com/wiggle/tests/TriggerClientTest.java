package com.wiggle.tests;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.ForFlow;
import com.wiggle.client.worker.PermanentActivityException;
import com.wiggle.client.worker.Worker;
import com.wiggle.client.worker.WorkerOptions;
import com.wiggle.core.InstanceView;
import com.wiggle.core.Json;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Triggers over gRPC: a failed payment starts its refund, run by the leader's housekeeping. */
class TriggerClientTest {

    interface PaySteps {
        Map<String, Object> charge(Map<String, Object> ctx);
    }

    interface RefundSteps {
        Map<String, Object> refund(Map<String, Object> ctx);
    }

    @ForFlow("trgc-payment")
    static final class PayH {
        public Map<String, Object> charge(Map<String, Object> c) { throw new PermanentActivityException("card declined"); }
    }

    @ForFlow("trgc-refund")
    static final class RefundH {
        public Map<String, Object> refund(Map<String, Object> c) {
            Map<String, Object> out = new java.util.HashMap<>(c);
            out.put("refunded", c.get("orderId"));
            return out;
        }
    }

    private static ServerConfig config() {
        return new ServerConfig(TestPorts.free(), "trigger-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    @Test @DisplayName("a failed payment starts a refund that sees the payment's context and why it failed")
    void failureStartsRefund() throws Exception {
        FlowSpec payment = FlowSpec.define("trgc-payment", 1, Map.class, PaySteps.class, (f, s) -> f.thenApply(s::charge));
        FlowSpec refund = FlowSpec.define("trgc-refund", 1, Map.class, RefundSteps.class, (f, s) -> f.thenApply(s::refund));
        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker worker = new Worker(client, "w-trigger", WorkerOptions.defaults().withConcurrency(2))
                     .registerHandler(new PayH()).registerHandler(new RefundH())) {
            client.register(payment);
            client.register(refund);
            worker.start();

            String id = client.createTrigger("trgc-refund", "trgc-payment", List.of("wf.failed"), true);
            List<WiggleClient.TriggerInfo> listed = client.triggers();
            assertEquals(1, listed.size());
            assertEquals(List.of("wf.failed"), listed.get(0).eventTypes());
            assertTrue(listed.get(0).includeContext());

            String pay = client.start("trgc-payment", Map.of("orderId", "o-7"), null, "order-7");
            assertEquals("FAILED", client.awaitCompletion(pay, Duration.ofSeconds(20)).status());

            InstanceView started = null;
            long deadline = System.currentTimeMillis() + 20_000;
            while (started == null && System.currentTimeMillis() < deadline) {
                List<InstanceView> refunds = server.engine().list("trgc-refund", null, 10);
                if (!refunds.isEmpty()) started = refunds.get(0);
                else Thread.sleep(50);
            }
            assertTrue(started != null, "the leader started the refund");
            InstanceView done = client.awaitCompletion(started.id(), Duration.ofSeconds(20));
            assertEquals("COMPLETED", done.status());
            Map<String, Object> ctx = Json.asObject(done.context());
            assertEquals("o-7", ctx.get("refunded"), "the refund ran on the payment's context");
            Map<String, Object> fired = Json.asObject(ctx.get("trigger"));
            assertEquals(pay, fired.get("instanceId"));
            assertEquals("wf.failed", fired.get("event"));
            assertTrue(String.valueOf(Json.asObject(fired.get("payload")).get("error")).contains("card declined"),
                    "why the source failed: " + fired);

            client.deleteTrigger(id);
            assertTrue(client.triggers().isEmpty());
            assertThrows(RuntimeException.class,
                    () -> client.createTrigger("trgc-refund", "trgc-payment", List.of("wf.nope"), false),
                    "an unknown lifecycle type is refused");
        }
    }
}