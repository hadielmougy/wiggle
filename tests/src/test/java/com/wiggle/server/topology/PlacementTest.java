package com.wiggle.server.topology;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.core.ShardIds;
import com.wiggle.postgres.PostgresStorageFactory;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Where new instances go: each generation's weights, exactly, and a switch at its activeFrom. */
class PlacementTest {

    private static final long T1 = Instant.parse("2026-10-01T00:00:00Z").toEpochMilli();
    private static final long T2 = Instant.parse("2026-11-01T00:00:00Z").toEpochMilli();

    private static String doc(String url0, String url1) {
        return """
                {
                  "generations": [
                    { "id": 1, "activeFrom": "2026-10-01T00:00:00Z", "weights": { "0": 1, "1": 3 } },
                    { "id": 2, "activeFrom": "2026-11-01T00:00:00Z", "weights": { "0": 1, "2": 1 } }
                  ],
                  "shards": [
                    { "id": 0, "state": "ACTIVE", "roles": ["instances", "home"], "primary": { "url": "%s" } },
                    { "id": 1, "state": "ACTIVE", "roles": ["instances"], "primary": { "url": "%s" } },
                    { "id": 2, "state": "ACTIVE", "roles": ["instances"], "primary": { "url": "%s" } }
                  ]
                }
                """.formatted(url0, url1, url1.replace("one", "two"));
    }

    private static Map<Integer, Integer> picks(Placement p, int n) {
        Map<Integer, Integer> count = new HashMap<>();
        for (int i = 0; i < n; i++) count.merge(p.nextShard(), 1, Integer::sum);
        return count;
    }

    @Test @DisplayName("over any run of picks, every shard gets its weighted share to within one")
    void exactShares() {
        AtomicLong now = new AtomicLong(T1);
        Placement p = new Placement(TopologyParser.parse(doc("a", "b-one"), Map.of()), now::get);
        assertEquals(Map.of(0, 1, 1, 3), picks(p, 4));
        assertEquals(Map.of(0, 25, 1, 75), picks(p, 100));
        Map<Integer, Integer> seven = picks(p, 7);
        assertTrue(Math.abs(seven.get(1) - 7 * 3 / 4.0) <= 1, seven.toString());
    }

    @Test @DisplayName("a new generation takes over at its activeFrom, not before")
    void switchesAtActiveFrom() {
        AtomicLong now = new AtomicLong(T2 - 1);
        Placement p = new Placement(TopologyParser.parse(doc("a", "b-one"), Map.of()), now::get);
        assertEquals(Map.of(0, 1, 1, 3), picks(p, 4), "still generation 1");
        now.set(T2);
        assertEquals(Map.of(0, 2, 2, 2), picks(p, 4), "generation 2: shard 1 gets nothing new");
        assertTrue(ShardIds.shardOf(p.next()).isPresent());
    }

    interface Steps { Map<String, Object> work(Map<String, Object> ctx); }

    @Test @DisplayName("a server started from a topology document runs on every shard it names")
    void serverFromDocument() throws Exception {
        String base = "jdbc:h2:mem:topo-" + System.nanoTime();
        // generation 1 has begun and generation 2 has not, whatever the date this runs on
        Topology t = TopologyParser.parse(doc(base + "-zero;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
                base + "-one;MODE=PostgreSQL;DB_CLOSE_DELAY=-1")
                .replace("2026-10-01T00:00:00Z", "2000-01-01T00:00:00Z")
                .replace("2026-11-01T00:00:00Z", "2999-01-01T00:00:00Z"), Map.of());
        ServerConfig config = new ServerConfig(0, "topo-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10)).withTopology(t);
        FlowSpec flow = FlowSpec.define("topo", 1, Map.class, Steps.class, (f, s) -> f.thenApply(s::work));

        try (WiggleServer server = new WiggleServer(config, new PostgresStorageFactory()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            client.register(flow);
            Map<Integer, Integer> landed = new HashMap<>();
            for (int i = 0; i < 8; i++) {
                String id = client.start(flow, Map.of());
                landed.merge(ShardIds.shardOf(id).getAsInt(), 1, Integer::sum);
                assertEquals("RUNNING", client.instance(id).status(), "readable on the shard it landed on");
            }
            assertEquals(Map.of(0, 2, 1, 6), landed, "generation 1 weights 1:3");
            assertEquals(8, client.listInstances("topo", null, 100).size(), "listed across both shards");
        }
    }
}
