package com.wiggle.tests;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.CompensableActivity;
import com.wiggle.client.worker.Compensation;
import com.wiggle.client.worker.ForFlow;
import com.wiggle.client.worker.PermanentActivityException;
import com.wiggle.client.worker.Worker;
import com.wiggle.client.worker.WorkerOptions;
import com.wiggle.core.EventView;
import com.wiggle.core.ExecutionMode;
import com.wiggle.core.InstanceView;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The event log records every instance lifecycle transition in the transaction that made it,
 * so what the log says of an instance is what the instance row says, in every execution mode.
 */
class EventLogTest {

    interface Steps {
        Map<String, Object> a(Map<String, Object> ctx);
        Map<String, Object> b(Map<String, Object> ctx);
    }

    interface FailingSteps {
        Map<String, Object> a(Map<String, Object> ctx);
        Map<String, Object> boom(Map<String, Object> ctx);
    }

    interface SagaSteps {
        CompensableActivity<Map<String, Object>, Map<String, Object>> reserve();
        Map<String, Object> boom(Map<String, Object> ctx);
    }

    interface ParkedSteps {
        Map<String, Object> a(Map<String, Object> ctx);
        Map<String, Object> after(Map<String, Object> ctx);
    }

    @ForFlow("evt-linear")
    static final class LinearH {
        public Map<String, Object> a(Map<String, Object> c) { return c; }
        public Map<String, Object> b(Map<String, Object> c) { return c; }
    }

    @ForFlow("evt-failing")
    static final class FailingH {
        public Map<String, Object> a(Map<String, Object> c) { return c; }
        public Map<String, Object> boom(Map<String, Object> c) { throw new PermanentActivityException("card declined"); }
    }

    @ForFlow("evt-saga")
    static final class SagaH {
        public CompensableActivity<Map<String, Object>, Map<String, Object>> reserve() {
            return new CompensableActivity<>() {
                public Map<String, Object> execute(Map<String, Object> c) { return c; }
                public void compensate(Compensation<Map<String, Object>, Map<String, Object>> c) { }
            };
        }
        public Map<String, Object> boom(Map<String, Object> c) { throw new PermanentActivityException("no stock"); }
    }

    @ForFlow("evt-parked")
    static final class ParkedH {
        public Map<String, Object> a(Map<String, Object> c) { return c; }
        public Map<String, Object> after(Map<String, Object> c) { return c; }
    }

    private static ServerConfig config() {
        return new ServerConfig(TestPorts.free(), "events-node", null, null, null, 4,
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

    @Test @DisplayName("a run's lifecycle lands in the log as it happens: started, then how it ended, in every mode")
    void lifecycle() throws Exception {
        for (ExecutionMode mode : List.of(ExecutionMode.SERVER, ExecutionMode.LOCAL_SYNC, ExecutionMode.LOCAL_ASYNC)) {
            FlowSpec linear = FlowSpec.define("evt-linear", 1, Map.class, Steps.class,
                    (f, s) -> Modes.in(f, mode).thenApply(s::a).thenApply(s::b));
            FlowSpec failing = FlowSpec.define("evt-failing", 1, Map.class, FailingSteps.class,
                    (f, s) -> f.thenApply(s::a).thenApply(s::boom));
            FlowSpec saga = FlowSpec.define("evt-saga", 1, Map.class, SagaSteps.class,
                    (f, s) -> f.thenApplyCompensable(s::reserve).thenApply(s::boom));
            FlowSpec parked = FlowSpec.define("evt-parked", 1, Map.class, ParkedSteps.class,
                    (f, s) -> f.thenApply(s::a).thenAwait("approval").thenApply(s::after));
            try (WiggleServer server = new WiggleServer(config()).start();
                 WiggleClient client = new WiggleClient(server.baseUrl());
                 Worker worker = new Worker(client, "w-events", WorkerOptions.defaults().withConcurrency(2))
                         .registerHandler(new LinearH()).registerHandler(new FailingH())
                         .registerHandler(new SagaH()).registerHandler(new ParkedH())) {
                for (FlowSpec spec : List.of(linear, failing, saga, parked)) client.register(spec);
                worker.start();

                String id = client.start("evt-linear", Map.of(), null, "order-" + mode);
                InstanceView v = client.awaitCompletion(id, Duration.ofSeconds(20));
                assertEquals("COMPLETED", v.status(), mode.toString());
                List<EventView> events = of(server, id);
                assertEquals(List.of("wf.started", "wf.completed"), types(events), mode.toString());
                assertTrue(events.get(0).seq() < events.get(1).seq(), mode + ": the log is ordered by seq");
                assertEquals("order-" + mode, events.get(0).correlationId(), mode + ": the run's key rides every entry");
                assertEquals("evt-linear", events.get(1).workflow());
                if (mode != ExecutionMode.SERVER) continue;

                String failed = client.start(failing, Map.of());
                assertEquals("FAILED", client.awaitCompletion(failed, Duration.ofSeconds(20)).status());
                List<EventView> failure = of(server, failed);
                assertEquals(List.of("wf.started", "wf.failed"), types(failure));
                assertTrue(String.valueOf(failure.get(1).payload().get("error")).contains("card declined"),
                        "the failure's error is the payload: " + failure.get(1).payload());

                String undone = client.start(saga, Map.of());
                assertEquals("COMPENSATED", client.awaitCompletion(undone, Duration.ofSeconds(20)).status());
                List<EventView> reverse = of(server, undone);
                assertEquals(List.of("wf.started", "wf.compensating", "wf.compensated"), types(reverse));
                assertTrue(String.valueOf(reverse.get(1).payload().get("error")).contains("no stock"));

                String waiting = client.start(parked, Map.of());
                Thread.sleep(500);
                client.cancel(waiting, "operator said so");
                assertEquals("CANCELLED", client.awaitCompletion(waiting, Duration.ofSeconds(20)).status());
                List<EventView> cancelled = of(server, waiting);
                assertEquals(List.of("wf.started", "wf.cancelled"), types(cancelled));
                assertEquals("operator said so", cancelled.get(1).payload().get("reason"));

                List<EventView> all = server.engine().events(0, 1_000);
                for (int i = 1; i < all.size(); i++) {
                    assertTrue(all.get(i - 1).seq() < all.get(i).seq(), "seq strictly increases across the log");
                }
                assertTrue(all.stream().allMatch(e -> e.type().startsWith("wf.")), "lifecycle entries carry the reserved prefix");
                assertEquals(0, server.engine().trimEvents(1_000), "nothing is older than the retention window");
            }
        }
    }

    @Test @DisplayName("retention drops entries older than the window; with no consumer, age alone decides")
    void retention() throws Exception {
        System.setProperty("wiggle.events.retentionMillis", "1");
        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker worker = new Worker(client, "w-retain", WorkerOptions.defaults()).registerHandler(new LinearH())) {
            FlowSpec linear = FlowSpec.define("evt-linear", 1, Map.class, Steps.class,
                    (f, s) -> f.thenApply(s::a).thenApply(s::b));
            client.register(linear);
            worker.start();
            String id = client.start(linear, Map.of());
            client.awaitCompletion(id, Duration.ofSeconds(20));
            assertEquals(2, of(server, id).size());
            Thread.sleep(20);
            assertEquals(2, server.engine().trimEvents(1_000), "both entries aged out");
            assertTrue(server.engine().events(0, 1_000).isEmpty());
        } finally {
            System.clearProperty("wiggle.events.retentionMillis");
        }
    }
}
