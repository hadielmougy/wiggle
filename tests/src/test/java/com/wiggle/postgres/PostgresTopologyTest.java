package com.wiggle.postgres;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.core.ShardIds;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import com.wiggle.server.topology.Topology;
import com.wiggle.server.topology.TopologyParser;
import com.wiggle.tests.TestDb;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A server on two real PostgreSQL databases, configured by a topology document: each database is
 * claimed for its shard, the registry lands on home, and instances land where their ids say. Opt-in
 * with {@code WIGGLE_TEST_PG_URL}; the user must be allowed to create databases.
 */
@EnabledIfEnvironmentVariable(named = "WIGGLE_TEST_PG_URL", matches = ".+")
class PostgresTopologyTest {

    interface Steps { Map<String, Object> work(Map<String, Object> ctx); }

    private final String suffix = Long.toHexString(System.nanoTime());
    private final List<String> databases = List.of("wiggle_topo_a_" + suffix, "wiggle_topo_b_" + suffix);

    private static Connection admin() throws SQLException {
        return DriverManager.getConnection(TestDb.url("PG"), TestDb.user("PG"), TestDb.password("PG"));
    }

    /** {@code WIGGLE_TEST_PG_URL} with its database swapped for {@code db}. */
    private static String urlOf(String db) {
        String url = TestDb.url("PG");
        int q = url.indexOf('?');
        String path = q < 0 ? url : url.substring(0, q);
        return path.substring(0, path.lastIndexOf('/') + 1) + db + (q < 0 ? "" : url.substring(q));
    }

    @BeforeEach
    void createDatabases() throws SQLException {
        try (Connection c = admin(); Statement s = c.createStatement()) {
            for (String db : databases) s.execute("CREATE DATABASE " + db);
        }
    }

    @AfterEach
    void dropDatabases() throws SQLException {
        try (Connection c = admin(); Statement s = c.createStatement()) {
            for (String db : databases) s.execute("DROP DATABASE IF EXISTS " + db + " WITH (FORCE)");
        }
    }

    private int count(String db, String sql) throws SQLException {
        try (Connection c = DriverManager.getConnection(urlOf(db), TestDb.user("PG"), TestDb.password("PG"));
             Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getInt(1);
        }
    }

    @Test @DisplayName("each database is claimed for its shard and holds exactly the instances whose ids name it")
    void twoDatabases() throws Exception {
        Topology t = TopologyParser.parse("""
                {
                  "defaults": { "user": "${U}", "password": "${P}", "pool": 4 },
                  "generations": [ { "id": 1, "activeFrom": "2000-01-01T00:00:00Z", "weights": { "0": 1, "1": 1 } } ],
                  "shards": [
                    { "id": 0, "state": "ACTIVE", "roles": ["instances", "home"], "primary": { "url": "%s" } },
                    { "id": 1, "state": "ACTIVE", "roles": ["instances"], "primary": { "url": "%s" } }
                  ]
                }
                """.formatted(urlOf(databases.get(0)), urlOf(databases.get(1))),
                Map.of("U", TestDb.user("PG"), "P", TestDb.password("PG")));
        ServerConfig config = new ServerConfig(0, "pg-topo", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10)).withTopology(t);
        FlowSpec flow = FlowSpec.define("pg-topo", 1, Map.class, Steps.class, (f, s) -> f.thenApply(s::work));

        Map<Integer, Integer> minted = new HashMap<>();
        try (WiggleServer server = new WiggleServer(config, new PostgresStorageFactory()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            client.register(flow);
            for (int i = 0; i < 6; i++) minted.merge(ShardIds.shardOf(client.start(flow, Map.of())).getAsInt(), 1, Integer::sum);
        }
        assertEquals(Map.of(0, 3, 1, 3), minted);
        for (int shard = 0; shard < 2; shard++) {
            String db = databases.get(shard);
            assertEquals(shard, count(db, "SELECT shard_id FROM wf_shard WHERE k='self'"), db + " is claimed for its shard");
            assertEquals(3, count(db, "SELECT COUNT(*) FROM wf_instance"), db + " holds its own instances");
            assertEquals(3, count(db, "SELECT COUNT(*) FROM wf_instance WHERE id LIKE 'wfi.s" + shard + ".%'"));
            assertEquals(true, count(db, "SELECT COUNT(*) FROM wf_graph_node WHERE workflow='pg-topo'") > 0,
                    db + " has the graph, registered on every shard");
        }
        assertEquals(2, count(databases.get(0), "SELECT COUNT(*) FROM wf_shard_registry"), "the registry is on home");
        assertEquals(0, count(databases.get(1), "SELECT COUNT(*) FROM wf_shard_registry"), "and only there");
    }
}
