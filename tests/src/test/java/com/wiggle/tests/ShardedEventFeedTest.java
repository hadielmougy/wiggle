package com.wiggle.tests;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.WiggleClient.WiggleApiException;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.core.EventView;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import com.wiggle.server.store.InMemoryStorage;
import com.wiggle.server.store.ShardedStorage;
import com.wiggle.server.store.Storage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The event feed over two shards, through gRPC: every shard keeps its own log, a poll merges them,
 * and a consumer acknowledges with the cursor an entry carries.
 */
class ShardedEventFeedTest {

    interface Steps { Map<String, Object> work(Map<String, Object> ctx); }

    private static final FlowSpec FLOW = FlowSpec.define("feed-sharded", 1, Map.class, Steps.class,
            (f, s) -> f.thenApply(s::work));

    private static WiggleServer server() throws Exception {
        ServerConfig config = new ServerConfig(TestPorts.free(), "feed-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
        return new WiggleServer(config, c -> {
            Map<Integer, Storage> shards = new LinkedHashMap<>();
            shards.put(1, new InMemoryStorage());
            shards.put(0, new InMemoryStorage());
            return new ShardedStorage(shards, 0);
        }).start();
    }

    /** Starts {@code n} instances, alternating shards, and waits out the feed's visibility window. */
    private static void start(WiggleClient client, int n) throws InterruptedException {
        for (int i = 0; i < n; i++) client.start(FLOW, Map.of());
        Thread.sleep(120);
    }

    @Test @DisplayName("a poll merges every shard's log oldest first, and an ack by cursor moves the consumer on")
    void readsEveryShard() throws Exception {
        try (WiggleServer server = server(); WiggleClient client = new WiggleClient(server.baseUrl())) {
            client.register(FLOW);
            start(client, 4);
            List<EventView> batch = client.pollEvents("billing", 100, 1_000, -1);
            assertEquals(4, batch.size(), "one wf.started per instance");
            Set<Integer> shards = new HashSet<>();
            for (int i = 0; i < batch.size(); i++) {
                shards.add(batch.get(i).shard());
                assertNotNull(batch.get(i).cursor());
                if (i > 0) assertTrue(batch.get(i - 1).createdAt() <= batch.get(i).createdAt(), "oldest first");
            }
            assertEquals(Set.of(0, 1), shards);

            client.ackEvents("billing", batch.getLast().cursor());
            assertTrue(client.pollEvents("billing", 100, 200, -1).isEmpty(), "everything acknowledged");
            start(client, 2);
            assertEquals(2, client.pollEvents("billing", 100, 1_000, -1).size(), "only what came after");
        }
    }

    @Test @DisplayName("acking part of a batch serves the rest again, on both shards")
    void aPartialAckRedelivers() throws Exception {
        try (WiggleServer server = server(); WiggleClient client = new WiggleClient(server.baseUrl())) {
            client.register(FLOW);
            start(client, 4);
            List<EventView> batch = client.pollEvents("partial", 100, 1_000, -1);
            client.ackEvents("partial", batch.get(1).cursor());
            List<EventView> again = client.pollEvents("partial", 100, 1_000, -1);
            assertEquals(batch.subList(2, 4).stream().map(e -> e.shard() + ":" + e.seq()).toList(),
                    again.stream().map(e -> e.shard() + ":" + e.seq()).toList());
        }
    }

    @Test @DisplayName("a sharded log refuses a bare seq: to ack, and to register after")
    void aBareSeqIsRefused() throws Exception {
        try (WiggleServer server = server(); WiggleClient client = new WiggleClient(server.baseUrl())) {
            client.register(FLOW);
            assertEquals(400, assertThrows(WiggleApiException.class, () -> client.ackEvents("c", 5L)).status());
            assertEquals(400, assertThrows(WiggleApiException.class,
                    () -> client.pollEvents("d", 10, 100, 5)).status());
            assertEquals(400, assertThrows(WiggleApiException.class,
                    () -> client.ackEvents("c", "not-a-cursor")).status());
        }
    }

    @Test @DisplayName("registering at the tail skips what every shard already holds")
    void tailRegistration() throws Exception {
        try (WiggleServer server = server(); WiggleClient client = new WiggleClient(server.baseUrl())) {
            client.register(FLOW);
            start(client, 2);
            assertTrue(client.pollEvents("tail", 100, 200, 0).isEmpty());
            start(client, 2);
            assertEquals(2, client.pollEvents("tail", 100, 1_000, 0).size());
        }
    }

    @Test @DisplayName("retention trims each shard only as far as every consumer has acknowledged on that shard")
    void retentionIsPerShard() throws Exception {
        System.setProperty("wiggle.events.retentionMillis", "0");
        try (WiggleServer server = server(); WiggleClient client = new WiggleClient(server.baseUrl())) {
            client.register(FLOW);
            start(client, 4);
            List<EventView> batch = client.pollEvents("keeper", 100, 1_000, -1);
            client.ackEvents("keeper", batch.getFirst().cursor());   // one entry, on one shard
            assertEquals(1, server.engine().trimEvents(100), "only the acknowledged entry may go");
            assertEquals(3, server.engine().events(0, 100).size());
        } finally {
            System.clearProperty("wiggle.events.retentionMillis");
        }
    }
}
