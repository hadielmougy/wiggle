package com.wiggle.tests;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.ForFlow;
import com.wiggle.client.worker.Worker;
import com.wiggle.client.worker.WorkerOptions;
import com.wiggle.core.EventView;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The event feed: a consumer is a named cursor over the log, delivery is at-least-once, and an
 * ack is the only thing that moves the cursor on.
 */
class EventFeedTest {

    interface Steps {
        Map<String, Object> a(Map<String, Object> ctx);
    }

    @ForFlow("feed-flow")
    static final class H {
        public Map<String, Object> a(Map<String, Object> c) { return c; }
    }

    private static ServerConfig config() {
        return new ServerConfig(TestPorts.free(), "feed-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    private static FlowSpec spec() {
        return FlowSpec.define("feed-flow", 1, Map.class, Steps.class, (f, s) -> f.thenApply(s::a));
    }

    private static List<String> types(List<EventView> events) {
        return events.stream().map(EventView::type).toList();
    }

    /**
     * Polls until the consumer is offered {@code expected} entries. A poll returns as soon as
     * anything is visible, and the newest appends are held back briefly, so a run's entries can
     * arrive across two polls; the cursor does not move without an ack, so each poll re-offers
     * everything behind it.
     */
    private static List<EventView> drain(WiggleClient client, String consumer, int expected, long startFrom) {
        long deadline = System.currentTimeMillis() + 5_000;
        List<EventView> batch = List.of();
        while (System.currentTimeMillis() < deadline && batch.size() < expected) {
            batch = client.pollEvents(consumer, 50, 1_000, startFrom);
        }
        return batch;
    }

    @Test @DisplayName("a consumer reads the log from where it asked, and only an ack moves it on")
    void pullAndAck() throws Exception {
        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker worker = new Worker(client, "w-feed", WorkerOptions.defaults()).registerHandler(new H())) {
            FlowSpec spec = spec();
            client.register(spec);
            worker.start();
            String first = client.start(spec, Map.of());
            client.awaitCompletion(first, Duration.ofSeconds(20));

            // -1: everything still retained, oldest first.
            List<EventView> batch = drain(client, "billing", 2, -1);
            assertEquals(List.of("wf.started", "wf.completed"), types(batch), "the whole run, in order");
            assertEquals(first, batch.get(0).instanceId());

            // No ack yet: the same entries are served again (at-least-once).
            assertEquals(types(batch), types(client.pollEvents("billing", 50, 2_000, -1)),
                    "an unacknowledged batch comes back");

            long through = batch.get(batch.size() - 1).seq();
            client.ackEvents("billing", through);
            assertTrue(client.pollEvents("billing", 10, 200, -1).isEmpty(), "acknowledged entries are behind the cursor");

            // A second run is served to the acknowledged consumer, and start_from is ignored now.
            String second = client.start(spec, Map.of());
            client.awaitCompletion(second, Duration.ofSeconds(20));
            List<EventView> next = drain(client, "billing", 2, 0);
            assertEquals(List.of("wf.started", "wf.completed"), types(next));
            assertEquals(second, next.get(0).instanceId(), "the cursor carried on, it did not jump to the tail");
            assertTrue(next.get(0).seq() > through, "seq only goes forward");

            // A consumer registered at the tail sees nothing of what is already there.
            assertTrue(client.pollEvents("audit", 10, 200, 0).isEmpty(), "a tail cursor starts empty");
            String third = client.start(spec, Map.of());
            client.awaitCompletion(third, Duration.ofSeconds(20));
            List<EventView> tail = drain(client, "audit", 1, 0);
            assertEquals(third, tail.get(0).instanceId(), "only what was appended after it registered");

            // Cursors are independent: billing has not acked the second run.
            assertEquals(second, drain(client, "billing", 1, 0).get(0).instanceId(),
                    "one consumer's ack is not another's");

            // max bounds the batch, and the payload survives the wire.
            List<EventView> one = client.pollEvents("audit", 1, 2_000, 0);
            assertEquals(1, one.size(), "max bounds the batch");
            assertEquals("feed-flow", one.get(0).workflow());
            assertNotNull(one.get(0).payload(), "a payload is always a map, empty when the transition carries nothing");

            WiggleClient.WiggleApiException blank = assertThrows(WiggleClient.WiggleApiException.class,
                    () -> client.pollEvents("  ", 10, 100, -1));
            assertTrue(blank.getMessage().contains("consumer name"), blank.getMessage());
        }
    }

    @Test @DisplayName("a long poll waits for an entry and returns it, or gives up empty at its deadline")
    void longPoll() throws Exception {
        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker worker = new Worker(client, "w-feed-wait", WorkerOptions.defaults()).registerHandler(new H())) {
            FlowSpec spec = spec();
            client.register(spec);
            worker.start();
            client.pollEvents("waiter", 10, 200, 0);   // register at the tail

            long emptyStart = System.currentTimeMillis();
            assertTrue(client.pollEvents("waiter", 10, 600, 0).isEmpty(), "nothing appended, nothing served");
            assertTrue(System.currentTimeMillis() - emptyStart >= 500, "the poll waited for its deadline");

            Thread starter = new Thread(() -> {
                try { Thread.sleep(300); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                client.start(spec, Map.of());
            });
            starter.start();
            List<EventView> woken = client.pollEvents("waiter", 10, 10_000, 0);
            starter.join();
            assertFalse(woken.isEmpty(), "the poll returned as soon as an entry became visible");
            assertEquals("wf.started", woken.get(0).type());
        }
    }

    @Test @DisplayName("retention keeps what the slowest consumer has not acknowledged")
    void retentionWaitsForConsumers() throws Exception {
        System.setProperty("wiggle.events.retentionMillis", "1");
        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker worker = new Worker(client, "w-feed-retain", WorkerOptions.defaults()).registerHandler(new H())) {
            FlowSpec spec = spec();
            client.register(spec);
            worker.start();
            client.awaitCompletion(client.start(spec, Map.of()), Duration.ofSeconds(20));

            List<EventView> all = drain(client, "slow", 2, -1);
            assertEquals(2, all.size());
            client.ackEvents("slow", all.get(0).seq());   // acknowledge the first entry only
            Thread.sleep(20);

            assertEquals(1, server.engine().trimEvents(100), "only what the cursor has passed is trimmed");
            List<EventView> left = server.engine().events(0, 100);
            assertEquals(1, left.size(), "the unacknowledged entry stayed");
            assertEquals(all.get(1).seq(), left.get(0).seq());

            client.ackEvents("slow", all.get(1).seq());
            assertEquals(1, server.engine().trimEvents(100), "acknowledged, so now it goes");
            assertTrue(server.engine().events(0, 100).isEmpty());
        } finally {
            System.clearProperty("wiggle.events.retentionMillis");
        }
    }
}
