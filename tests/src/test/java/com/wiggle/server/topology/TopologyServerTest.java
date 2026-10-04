package com.wiggle.server.topology;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.WiggleClient.WiggleApiException;
import com.wiggle.postgres.PostgresStorageFactory;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import com.wiggle.server.store.Rows.ServerNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** A server started from a topology document, seen from outside: over gRPC and in its node row. */
class TopologyServerTest {

    private static ServerConfig config(Topology t) {
        return new ServerConfig(0, "topo-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(200), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10)).withTopology(t);
    }

    private static Topology topology() {
        String base = "jdbc:h2:mem:topo-srv-" + System.nanoTime();
        return TopologyParser.parse("""
                {
                  "generations": [
                    { "id": 4, "activeFrom": "2000-01-01T00:00:00Z", "weights": { "0": 1, "1": 1 } },
                    { "id": 5, "activeFrom": "2999-01-01T00:00:00Z", "weights": { "0": 1 } }
                  ],
                  "shards": [
                    { "id": 0, "state": "ACTIVE", "roles": ["instances", "home"], "primary": { "url": "%1$s-0;MODE=PostgreSQL;DB_CLOSE_DELAY=-1" } },
                    { "id": 1, "state": "ACTIVE", "roles": ["instances"], "primary": { "url": "%1$s-1;MODE=PostgreSQL;DB_CLOSE_DELAY=-1" } },
                    { "id": 2, "state": "RETIRED", "roles": ["instances"], "primary": { "url": "%1$s-2;MODE=PostgreSQL;DB_CLOSE_DELAY=-1" } }
                  ]
                }
                """.formatted(base), Map.of());
    }

    @Test @DisplayName("an id on a retired shard is NOT_FOUND over gRPC")
    void aRetiredShardIsNotFound() throws Exception {
        try (WiggleServer server = new WiggleServer(config(topology()), new PostgresStorageFactory()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            WiggleApiException e = assertThrows(WiggleApiException.class, () -> client.instance("wfi.s2.01k6abc"));
            assertEquals(404, e.status(), e.getMessage());
        }
    }

    @Test @DisplayName("a node publishes the newest generation it has loaded, not just the one in force")
    void aNodePublishesItsGeneration() throws Exception {
        try (WiggleServer server = new WiggleServer(config(topology()), new PostgresStorageFactory()).start()) {
            long deadline = System.currentTimeMillis() + 5_000;
            List<ServerNode> nodes = server.cluster().members();
            while (nodes.isEmpty() && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
                nodes = server.cluster().members();
            }
            assertEquals(5, nodes.getFirst().topologyGeneration);
        }
    }
}
