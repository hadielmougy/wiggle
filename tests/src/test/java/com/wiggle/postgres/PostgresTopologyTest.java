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

            Thread.sleep(120);   // the feed's visibility window
            List<com.wiggle.core.EventView> events = client.pollEvents("pg-feed", 100, 2_000, -1);
            assertEquals(6, events.size(), "every shard's log, in one poll");
            assertEquals(java.util.Set.of(0, 1), new java.util.HashSet<>(events.stream().map(com.wiggle.core.EventView::shard).toList()));
            client.ackEvents("pg-feed", events.getLast().cursor());
            assertEquals(0, client.pollEvents("pg-feed", 100, 300, -1).size(), "acknowledged on both shards");
        }
        assertEquals(1, count(databases.get(0), "SELECT COUNT(*) FROM wf_event_cursor_shard WHERE consumer='pg-feed' AND shard_id=1"),
                "the position on shard 1 is held on home");
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

    @Test @DisplayName("accounts, roles, sessions and their audit live on the auth shard, and only there")
    void authShard() throws Exception {
        Topology t = TopologyParser.parse("""
                {
                  "defaults": { "user": "${U}", "password": "${P}", "pool": 4 },
                  "generations": [ { "id": 1, "activeFrom": "2000-01-01T00:00:00Z", "weights": { "0": 1 } } ],
                  "shards": [
                    { "id": 0, "state": "ACTIVE", "roles": ["instances", "home"], "primary": { "url": "%s" } },
                    { "id": 1, "state": "ACTIVE", "roles": ["auth"], "primary": { "url": "%s" } }
                  ]
                }
                """.formatted(urlOf(databases.get(0)), urlOf(databases.get(1))),
                Map.of("U", TestDb.user("PG"), "P", TestDb.password("PG")));
        ServerConfig config = new ServerConfig(0, "pg-auth", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10)).withTopology(t);
        try (WiggleServer server = new WiggleServer(config, new PostgresStorageFactory()).start()) {
            server.accounts().create(null, "dana", "dana-password", List.of("admin"), java.util.Set.of(), true);
            server.accounts().openSession("dana", 60_000);
            assertEquals(true, server.accounts().account("dana").orElseThrow().passwordMatches("dana-password"));
        }
        String auth = databases.get(1), home = databases.get(0);
        assertEquals(1, count(auth, "SELECT shard_id FROM wf_shard WHERE k='self'"), "the auth database is claimed");
        assertEquals(1, count(auth, "SELECT COUNT(*) FROM wf_auth_user WHERE name='dana'"));
        assertEquals(1, count(auth, "SELECT COUNT(*) FROM wf_auth_user_role WHERE user_name='dana' AND role_name='admin'"));
        assertEquals(2, count(auth, "SELECT COUNT(*) FROM wf_auth_role WHERE builtin=1"));
        assertEquals(1, count(auth, "SELECT COUNT(*) FROM wf_auth_session WHERE user_name='dana'"));
        assertEquals(true, count(auth, "SELECT COUNT(*) FROM wf_auth_audit") >= 3);
        for (String table : List.of("wf_auth_user", "wf_auth_role", "wf_auth_session", "wf_auth_audit")) {
            assertEquals(0, count(home, "SELECT COUNT(*) FROM " + table), table + " is empty on home");
        }
        assertEquals(0, count(auth, "SELECT COUNT(*) FROM wf_instance"), "the auth shard holds no instances");
    }

    @Test @DisplayName("search documents live on the search shard, and only there, and full-text queries find them")
    void searchShard() throws Exception {
        Topology t = TopologyParser.parse("""
                {
                  "defaults": { "user": "${U}", "password": "${P}", "pool": 4 },
                  "generations": [ { "id": 1, "activeFrom": "2000-01-01T00:00:00Z", "weights": { "0": 1 } } ],
                  "shards": [
                    { "id": 0, "state": "ACTIVE", "roles": ["instances", "home"], "primary": { "url": "%s" } },
                    { "id": 1, "state": "ACTIVE", "roles": ["search"], "primary": { "url": "%s" } }
                  ]
                }
                """.formatted(urlOf(databases.get(0)), urlOf(databases.get(1))),
                Map.of("U", TestDb.user("PG"), "P", TestDb.password("PG")));
        ServerConfig config = new ServerConfig(0, "pg-search", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10)).withTopology(t);
        FlowSpec flow = FlowSpec.define("pg-search", 1, Map.class, Steps.class, (f, s) -> f.thenApply(s::work));
        try (WiggleServer server = new WiggleServer(config, new PostgresStorageFactory()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            client.register(flow);
            String ada = client.start("pg-search", Map.of("customer", "Ada Lovelace"), null, null);
            client.start("pg-search", Map.of("customer", "Alan Turing"), null, null);
            long deadline = System.currentTimeMillis() + 15_000;
            List<WiggleClient.SearchHit> hits = List.of();
            while (hits.isEmpty() && System.currentTimeMillis() < deadline) {
                Thread.sleep(200);
                hits = client.search("lovelace", null, null, null, null, 10, false).hits();
            }
            assertEquals(List.of(ada), hits.stream().map(WiggleClient.SearchHit::instanceId).toList());
            assertEquals(true, hits.getFirst().score() > 0, "ranked by the database's own full-text search");
            while (client.search("", null, null, null, null, 10, false).hits().size() < 2
                    && System.currentTimeMillis() < deadline) {
                Thread.sleep(100);   // the other instance is indexed in its own batch
            }
        }
        assertEquals(2, count(databases.get(1), "SELECT COUNT(*) FROM wf_search_doc"), "both instances are indexed");
        assertEquals(1, count(databases.get(1), "SELECT COUNT(*) FROM wf_search_doc WHERE tsv @@ plainto_tsquery('simple', 'turing')"));
        assertEquals(0, count(databases.get(0), "SELECT COUNT(*) FROM wf_search_doc"), "and nothing on the instance shard");
        assertEquals(0, count(databases.get(1), "SELECT COUNT(*) FROM wf_instance"), "nor instances on the search shard");
    }

    /** Where pgvector is installed: vectors in the native column, under the model's HNSW index. Skipped elsewhere. */
    @Test @DisplayName("semantic search on a pgvector search shard uses the database's own vector index")
    void pgvectorSearchShard() throws Exception {
        Topology t = TopologyParser.parse("""
                {
                  "defaults": { "user": "${U}", "password": "${P}", "pool": 4 },
                  "generations": [ { "id": 1, "activeFrom": "2000-01-01T00:00:00Z", "weights": { "0": 1 } } ],
                  "shards": [
                    { "id": 0, "state": "ACTIVE", "roles": ["instances", "home"], "primary": { "url": "%s" } },
                    { "id": 1, "state": "ACTIVE", "roles": ["search"], "primary": { "url": "%s" } }
                  ]
                }
                """.formatted(urlOf(databases.get(0)), urlOf(databases.get(1))),
                Map.of("U", TestDb.user("PG"), "P", TestDb.password("PG")));
        ServerConfig config = new ServerConfig(0, "pg-vector", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10)).withTopology(t)
                .withSearch(new ServerConfig.Search(false, Duration.ofDays(30), java.util.Set.of(), Duration.ofMillis(300)));
        FlowSpec flow = FlowSpec.define("pg-vector", 1, Map.class, Steps.class, (f, s) -> f.thenApply(s::work));
        try (WiggleServer server = new WiggleServer(config, new PostgresStorageFactory(),
                List.of(new com.wiggle.server.search.HashingEmbedder(64))).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            org.junit.jupiter.api.Assumptions.assumeTrue(count(databases.get(1),
                    "SELECT COUNT(*) FROM information_schema.columns WHERE table_name='wf_search_vec' AND column_name='vec'") == 1,
                    "pgvector is not installed on this PostgreSQL");
            client.register(flow);
            String damaged = client.start("pg-vector", Map.of("note", "parcel arrived damaged, wants a refund"), null, null);
            client.start("pg-vector", Map.of("note", "invoice settled early"), null, null);
            long deadline = System.currentTimeMillis() + 20_000;
            WiggleClient.SearchResult r = null;
            while (System.currentTimeMillis() < deadline) {
                try {
                    r = client.search("damaged parcel refund", null, null, null, null, 2, false, true);
                    if (r.hits().size() == 2) break;
                } catch (WiggleClient.WiggleApiException building) {
                    // the model's index is not complete yet
                }
                Thread.sleep(200);
            }
            assertEquals(damaged, r.hits().getFirst().instanceId());
            assertEquals("hashing-64", r.model());
        }
        String search = databases.get(1);
        assertEquals(2, count(search, "SELECT COUNT(*) FROM wf_search_vec WHERE vec IS NOT NULL AND model='hashing-64'"),
                "vectors are in the native column");
        assertEquals(1, count(search, "SELECT COUNT(*) FROM pg_indexes WHERE tablename='wf_search_vec' AND indexdef LIKE '%hnsw%'"));
        try (Connection c = DriverManager.getConnection(urlOf(search), TestDb.user("PG"), TestDb.password("PG"));
             Statement st = c.createStatement()) {
            st.execute("SET enable_seqscan = off");
            StringBuilder plan = new StringBuilder();
            float[] q = new float[64];
            q[0] = 1;
            try (ResultSet rs = st.executeQuery("EXPLAIN SELECT instance_id FROM wf_search_vec v WHERE v.model='hashing-64' "
                    + "AND v.vec IS NOT NULL ORDER BY (v.vec::vector(64) <=> CAST('" + com.wiggle.server.store.Vectors.literal(q)
                    + "' AS vector(64))) LIMIT 5")) {
                while (rs.next()) plan.append(rs.getString(1)).append('\n');
            }
            assertEquals(true, plan.toString().contains("ix_search_vec_"), "the query shape the store issues can use the HNSW index:\n" + plan);
        }
    }
}
