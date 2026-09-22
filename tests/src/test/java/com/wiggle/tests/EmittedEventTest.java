package com.wiggle.tests;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.ForFlow;
import com.wiggle.client.worker.PermanentActivityException;
import com.wiggle.client.worker.Step;
import com.wiggle.client.worker.Worker;
import com.wiggle.client.worker.WorkerOptions;
import com.wiggle.core.EmittedEvent;
import com.wiggle.core.EventView;
import com.wiggle.core.ExecutionMode;
import com.wiggle.core.InstanceView;
import com.wiggle.core.RetryPolicy;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a handler emits reaches the event log with the step that emitted it, and only if that
 * attempt completed: the append rides the transaction that settles the token.
 */
class EmittedEventTest {

    interface Steps {
        Map<String, Object> charge(Map<String, Object> ctx);
        Map<String, Object> ship(Map<String, Object> ctx);
    }

    interface OneStep {
        Map<String, Object> work(Map<String, Object> ctx);
    }

    @ForFlow("emit-flow")
    static final class H {
        public Map<String, Object> charge(Map<String, Object> c) {
            Step.emit("payment.captured", Map.of("amount", 4200L, "currency", "EUR"));
            return c;
        }
        public Map<String, Object> ship(Map<String, Object> c) {
            Step.emit("parcel.dispatched", Map.of("carrier", "dhl"));
            Step.emit("customer.notified", Map.of("channel", "email"));
            return c;
        }
    }

    /** Emits, then throws: the attempt's events must not survive it. */
    @ForFlow("emit-thrower")
    static final class Thrower {
        public Map<String, Object> work(Map<String, Object> c) {
            Step.emit("never.seen", Map.of("attempt", Step.attempt()));
            throw new PermanentActivityException("deliberate: nothing this attempt emitted may land");
        }
    }

    /** Emits and fails once, then emits and succeeds: the log keeps only the attempt that completed. */
    @ForFlow("emit-retrier")
    static final class Retrier {
        static final AtomicInteger ATTEMPTS = new AtomicInteger();
        public Map<String, Object> work(Map<String, Object> c) {
            int attempt = ATTEMPTS.incrementAndGet();
            Step.emit("invoice.issued", Map.of("attempt", (long) attempt));
            if (attempt == 1) throw new IllegalStateException("gateway timeout");
            return c;
        }
    }

    @ForFlow("emit-badpayload")
    static final class BadPayload {
        public Map<String, Object> work(Map<String, Object> c) {
            Step.emit("order.paid", "just-a-string");
            return c;
        }
    }

    @ForFlow("emit-reserved")
    static final class Reserved {
        public Map<String, Object> work(Map<String, Object> c) {
            Step.emit("wf.completed", Map.of("spoofed", true));
            return c;
        }
    }

    private static ServerConfig config() {
        return new ServerConfig(TestPorts.free(), "emit-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    private static List<EventView> of(WiggleServer server, String instanceId) {
        return server.engine().events(0, 1_000).stream().filter(e -> e.instanceId().equals(instanceId)).toList();
    }

    private static List<String> types(List<EventView> events) {
        return events.stream().map(EventView::type).toList();
    }

    @Test @DisplayName("what a handler emits lands in the log, named by its step, in every mode")
    void emitted() throws Exception {
        for (ExecutionMode mode : List.of(ExecutionMode.SERVER, ExecutionMode.LOCAL_SYNC, ExecutionMode.LOCAL_ASYNC)) {
            FlowSpec spec = FlowSpec.define("emit-flow", 1, Map.class, Steps.class,
                    (f, s) -> Modes.in(f, mode).thenApply(s::charge).thenApply(s::ship));
            try (WiggleServer server = new WiggleServer(config()).start();
                 WiggleClient client = new WiggleClient(server.baseUrl());
                 Worker worker = new Worker(client, "w-emit", WorkerOptions.defaults()).registerHandler(new H())) {
                client.register(spec);
                worker.start();
                String id = client.start(spec, Map.of());
                assertEquals("COMPLETED", client.awaitCompletion(id, Duration.ofSeconds(20)).status(), mode.toString());

                List<EventView> log = of(server, id);
                assertEquals(List.of("wf.started", "payment.captured", "parcel.dispatched",
                                "customer.notified", "wf.completed"), types(log),
                        mode + ": emitted entries sit in the log where they happened");

                EventView payment = log.get(1);
                assertEquals(4200L, ((Number) payment.payload().get("amount")).longValue(), mode.toString());
                assertEquals("EUR", payment.payload().get("currency"));
                assertEquals(spec.definition().startNode(), payment.nodeId(), mode + ": the step that emitted it");
                assertEquals(log.get(2).nodeId(), log.get(3).nodeId(), mode + ": both from the shipping step");
                assertNotEquals(payment.nodeId(), log.get(2).nodeId(), mode + ": and that is a different step");
                assertNull(log.getFirst().nodeId(), mode + ": a lifecycle entry names no step");
            }
        }
    }

    @Test @DisplayName("an attempt that throws leaves nothing behind, and its retry emits fresh")
    void onlyWhatCompleted() throws Exception {
        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker worker = new Worker(client, "w-emit-fail", WorkerOptions.defaults())
                     .registerHandler(new Thrower()).registerHandler(new Retrier())) {
            FlowSpec thrower = FlowSpec.define("emit-thrower", 1, Map.class, OneStep.class,
                    (f, s) -> f.thenApply(s::work));
            FlowSpec retrier = FlowSpec.define("emit-retrier", 1, Map.class, OneStep.class,
                    (f, s) -> f.thenApply(s::work, RetryPolicy.fixed(3, Duration.ofMillis(10))));
            client.register(thrower);
            client.register(retrier);
            worker.start();

            String failed = client.start(thrower, Map.of());
            assertEquals("FAILED", client.awaitCompletion(failed, Duration.ofSeconds(20)).status());
            assertEquals(List.of("wf.started", "wf.failed"), types(of(server, failed)),
                    "the attempt threw, so what it emitted rolled back with it");

            Retrier.ATTEMPTS.set(0);
            String retried = client.start(retrier, Map.of());
            assertEquals("COMPLETED", client.awaitCompletion(retried, Duration.ofSeconds(20)).status());
            List<EventView> log = of(server, retried);
            assertEquals(List.of("wf.started", "invoice.issued", "wf.completed"), types(log),
                    "one entry, from the attempt that completed");
            assertEquals(2L, ((Number) log.get(1).payload().get("attempt")).longValue(),
                    "and it is the retry's, not the failed attempt's");
        }
    }

    @Test @DisplayName("a payload that is not an object, and the reserved prefix, are refused")
    void refused() throws Exception {
        IllegalArgumentException reserved = assertThrows(IllegalArgumentException.class,
                () -> new EmittedEvent("wf.completed", Map.of()));
        assertTrue(reserved.getMessage().contains("reserved"), reserved.getMessage());
        assertThrows(IllegalArgumentException.class, () -> new EmittedEvent("  ", Map.of()));

        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker worker = new Worker(client, "w-emit-bad", WorkerOptions.defaults())
                     .registerHandler(new BadPayload()).registerHandler(new Reserved())) {
            FlowSpec bad = FlowSpec.define("emit-badpayload", 1, Map.class, OneStep.class,
                    (f, s) -> f.thenApply(s::work, RetryPolicy.none()));
            FlowSpec spoof = FlowSpec.define("emit-reserved", 1, Map.class, OneStep.class,
                    (f, s) -> f.thenApply(s::work, RetryPolicy.none()));
            client.register(bad);
            client.register(spoof);
            worker.start();

            InstanceView v = client.awaitCompletion(client.start(bad, Map.of()), Duration.ofSeconds(20));
            assertEquals("FAILED", v.status());
            assertTrue(v.error().contains("record or a map"), v.error());
            assertEquals(List.of("wf.started", "wf.failed"), types(of(server, v.id())));

            InstanceView s = client.awaitCompletion(client.start(spoof, Map.of()), Duration.ofSeconds(20));
            assertEquals("FAILED", s.status());
            assertTrue(s.error().contains("reserved"), s.error());
            assertTrue(types(of(server, s.id())).stream().noneMatch(t -> t.equals("wf.completed")),
                    "a handler cannot forge a lifecycle entry");
        }
    }
}
