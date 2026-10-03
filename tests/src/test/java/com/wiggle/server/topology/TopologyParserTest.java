package com.wiggle.server.topology;

import com.wiggle.server.store.ShardState;
import com.wiggle.server.topology.Topology.Role;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The topology document: what it says, what it inherits, and every way it is refused. */
class TopologyParserTest {

    private static final Map<String, String> ENV = Map.of("PG_USER", "wiggle", "PG_PASS", "s3cret", "S1_PASS", "other");

    private static final String DOC = """
            {
              "defaults": { "user": "${PG_USER}", "password": "${PG_PASS}", "pool": 32 },
              "generations": [
                { "id": 1, "activeFrom": "2026-10-01T00:00:00Z", "weights": { "0": 1, "1": 1 } },
                { "id": 2, "activeFrom": "2026-11-02T09:00:00Z", "weights": { "0": 1, "1": 1, "2": 3 } }
              ],
              "shards": [
                { "id": 0, "state": "ACTIVE", "roles": ["instances", "home"],
                  "primary": { "url": "jdbc:postgresql://pg-s0/wiggle" } },
                { "id": 1, "state": "ACTIVE", "roles": ["instances"], "password": "${S1_PASS}",
                  "primary": { "url": "jdbc:postgresql://pg-s1/wiggle", "pool": 8 } },
                { "id": 2, "state": "active", "roles": ["instances"],
                  "primary": { "url": "jdbc:postgresql://pg-s2/wiggle" } },
                { "id": 10, "state": "ACTIVE", "roles": ["search"] }
              ]
            }
            """;

    private static String with(String from, String to) {
        assertTrue(DOC.contains(from), "test fixture drifted: " + from);
        return DOC.replace(from, to);
    }

    private static IllegalArgumentException refused(String doc) {
        return assertThrows(IllegalArgumentException.class, () -> TopologyParser.parse(doc, ENV));
    }

    private static void refused(String doc, String says) {
        IllegalArgumentException e = refused(doc);
        assertTrue(e.getMessage().contains(says), e.getMessage());
    }

    @Test @DisplayName("shards, roles, states and generations read back as written")
    void reads() {
        Topology t = TopologyParser.parse(DOC, ENV);
        assertEquals(4, t.shards().size());
        assertEquals(0, t.home());
        assertEquals(Set.of(Role.INSTANCES, Role.HOME, Role.AUTH), t.shard(0).orElseThrow().roles(),
                "with no auth shard named, home carries auth");
        assertEquals(ShardState.ACTIVE, t.shard(2).orElseThrow().state(), "states read case-insensitively");
        assertEquals(2, t.generations().size());
        assertEquals(Map.of(0, 1, 1, 1, 2, 3), t.generations().get(1).weights());
    }

    @Test @DisplayName("a connection setting comes from the primary, else the shard, else the defaults")
    void settingsInherit() {
        Topology t = TopologyParser.parse(DOC, ENV);
        Topology.Connection s0 = t.shard(0).orElseThrow().primary();
        Topology.Connection s1 = t.shard(1).orElseThrow().primary();
        assertEquals("wiggle", s0.user());
        assertEquals("s3cret", s0.password());
        assertEquals(32, s0.pool());
        assertEquals("other", s1.password(), "the shard overrides the default");
        assertEquals(8, s1.pool(), "the primary overrides both");
        assertTrue(!s0.toString().contains("s3cret"), "a password never reaches a log line");
    }

    @Test @DisplayName("the generation in force is the newest that has begun, else the oldest")
    void generationAt() {
        Topology t = TopologyParser.parse(DOC, ENV);
        long gen2 = t.generations().get(1).activeFrom();
        assertEquals(1, t.generationAt(0).id(), "before any has begun, the oldest");
        assertEquals(1, t.generationAt(gen2 - 1).id());
        assertEquals(2, t.generationAt(gen2).id());
        assertEquals(2, t.newestGeneration().id());
    }

    @Test @DisplayName("an unset ${VAR} is refused, naming it")
    void unsetVariable() {
        refused(with("${S1_PASS}", "${NOPE}"), "${NOPE}");
    }

    @Test @DisplayName("structural mistakes are refused with what is wrong")
    void structure() {
        refused(with("\"id\": 2, \"state\"", "\"id\": 1, \"state\""), "listed twice");
        refused(with("[\"instances\", \"home\"]", "[\"instances\"]"), "home role");
        refused(with("[\"instances\"], \"password\"", "[\"instances\", \"home\"], \"password\""), "home role");
        refused(with("[\"search\"]", "[\"auth\"]").replace("[\"instances\"], \"password\"", "[\"instances\", \"auth\"], \"password\""), "auth role");
        refused(with("\"state\": \"ACTIVE\", \"roles\": [\"instances\", \"home\"]",
                "\"state\": \"DRAINING\", \"roles\": [\"instances\", \"home\"]"), "home shard is DRAINING");
        refused(with("\"state\": \"active\"", "\"state\": \"PAUSED\""), "PAUSED");
        refused(with("[\"search\"]", "[\"cache\"]"), "cache");
        refused(with("[\"search\"]", "[]"), "no roles");
        refused(with("{ \"url\": \"jdbc:postgresql://pg-s2/wiggle\" }", "{ }"), "primary url");
        refused(with("\"pool\": 8", "\"pool\": 0"), "pool");
        refused("{ not json", "not valid JSON");
    }

    @Test @DisplayName("generations are ordered, weigh only active instance shards, and give someone weight")
    void generations() {
        refused(with("\"id\": 2, \"activeFrom\"", "\"id\": 1, \"activeFrom\""), "must come after");
        refused(with("2026-11-02T09:00:00Z", "2026-09-01T00:00:00Z"), "must come after");
        refused(with("\"0\": 1, \"1\": 1, \"2\": 3", "\"0\": 0"), "no shard a positive weight");
        refused(with("\"2\": 3", "\"7\": 3"), "not listed");
        refused(with("\"2\": 3", "\"10\": 3"), "holds no instances");
        refused(with("\"state\": \"active\"", "\"state\": \"DRAINING\""), "which is DRAINING");
        refused(with("\"2\": 3", "\"2\": -1"), "non-negative");
        refused(with("2026-10-01T00:00:00Z", "yesterday"), "ISO-8601");
        refused(DOC.replaceAll("(?s)\"generations\": \\[.*?],\\s*\"shards\"", "\"generations\": [], \"shards\""),
                "no generations");
    }

    @Test @DisplayName("replicas are refused until read-replica support lands")
    void replicasNotYet() {
        refused(with("\"primary\": { \"url\": \"jdbc:postgresql://pg-s2/wiggle\" }",
                "\"primary\": { \"url\": \"jdbc:postgresql://pg-s2/wiggle\" }, \"replicas\": [ { \"url\": \"x\" } ]"),
                "replicas");
        assertThrows(IllegalArgumentException.class, () ->
                TopologyParser.fromEnvironment(Map.of("WIGGLE_JDBC_REPLICA_URLS", "jdbc:postgresql://r1/w")));
    }

    @Test @DisplayName("the environment names the document as a path or inline, and never alongside WIGGLE_JDBC_URL")
    void fromEnvironment() throws Exception {
        assertTrue(TopologyParser.fromEnvironment(Map.of()).isEmpty(), "no topology: one database, as before");
        assertEquals(4, TopologyParser.fromEnvironment(Map.of("WIGGLE_STORAGE_TOPOLOGY", DOC,
                "PG_USER", "u", "PG_PASS", "p", "S1_PASS", "q")).orElseThrow().shards().size());
        Path file = Files.createTempFile("topology", ".json");
        try {
            Files.writeString(file, DOC);
            assertEquals(4, TopologyParser.fromEnvironment(Map.of("WIGGLE_STORAGE_TOPOLOGY", file.toString(),
                    "PG_USER", "u", "PG_PASS", "p", "S1_PASS", "q")).orElseThrow().shards().size());
        } finally {
            Files.deleteIfExists(file);
        }
        IllegalArgumentException both = assertThrows(IllegalArgumentException.class, () ->
                TopologyParser.fromEnvironment(Map.of("WIGGLE_STORAGE_TOPOLOGY", DOC,
                        "WIGGLE_JDBC_URL", "jdbc:postgresql://db/wiggle")));
        assertTrue(both.getMessage().contains("WIGGLE_JDBC_URL"), both.getMessage());
    }
}
