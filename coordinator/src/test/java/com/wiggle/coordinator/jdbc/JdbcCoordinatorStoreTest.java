package com.wiggle.coordinator.jdbc;

import com.wiggle.placement.Ring;
import com.wiggle.election.ElectionStore;
import com.wiggle.election.LeaderElection;
import com.wiggle.election.Member;
import com.wiggle.server.coord.CoordDefinition;
import com.wiggle.server.coord.CoordNamespace;
import com.wiggle.server.coord.CoordNode;
import com.wiggle.server.coord.CoordPolicy;
import com.wiggle.server.coord.CoordinatorStore;
import com.wiggle.server.coord.ProvisionState;
import com.wiggle.server.coord.StorageConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The JDBC coordinator store against H2 (PostgreSQL mode) — the same contract the in-memory
 * reference gives, with the CAS atomicity that keeps a multi-node coordinator single-writer.
 */
class JdbcCoordinatorStoreTest {

    private JdbcCoordinatorStoreProvider provider;
    private CoordinatorStore store;

    // Runs on H2 (PostgreSQL mode) by default; against a real database when WIGGLE_TEST_PG_URL is set
    // (e.g. `docker compose up -d postgres`), which exercises the actual portability of the SQL.
    @BeforeEach void setUp() throws Exception {
        String pg = env("WIGGLE_TEST_PG_URL");
        String url, user, pw;
        if (pg != null && !pg.isBlank()) {
            url = pg;
            // Credentials fall back to the generic pair the rest of the suite uses, so setting
            // WIGGLE_TEST_DB_* and adding only WIGGLE_TEST_PG_URL works. This duplicates
            // com.wiggle.tests.TestDb rather than sharing it: :coordinator depends on :core and
            // :proto alone, and must not grow a dependency on the :tests module to borrow six lines.
            user = env("WIGGLE_TEST_PG_USER", "WIGGLE_TEST_DB_USER");
            pw = env("WIGGLE_TEST_PG_PASSWORD", "WIGGLE_TEST_DB_PASSWORD");
            dropCoordTables(url, user, pw);   // a clean slate per test on a persistent DB
        } else {
            url = "jdbc:h2:mem:coord-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
            user = "sa";
            pw = "";
        }
        provider = new JdbcCoordinatorStoreProvider(url, user, pw, 4);
        store = provider.coordinatorStore();
    }

    /** The first of {@code keys} that is set and non-blank, or null. */
    private static String env(String... keys) {
        for (String key : keys) {
            String v = System.getenv(key);
            if (v != null && !v.isBlank()) return v;
        }
        return null;
    }

    private static void dropCoordTables(String url, String user, String pw) throws Exception {
        try (var c = java.sql.DriverManager.getConnection(url, user, pw); var s = c.createStatement()) {
            for (String t : new String[]{"coord_policy", "coord_node", "coord_definition",
                    "coord_namespace", "coord_cell_binding", "coord_member"}) {
                s.execute("DROP TABLE IF EXISTS " + t);
            }
        }
    }

    @AfterEach void tearDown() throws Exception {
        store.close();
    }

    private static CoordPolicy policy(String ns, long epoch, long rev) {
        Map<Long, Ring.Epoch> epochs = Map.of(epoch, new Ring.Epoch(
                List.of(new Ring.Slot(0, "cellA", "eu"), new Ring.Slot(1, "cellB", "us")), Ring.Status.OPEN));
        return new CoordPolicy(ns, epoch, rev, epochs);
    }

    @Test @DisplayName("policy CAS: create-once, revision-guarded update, and the ring round-trips")
    void policyCas() {
        assertTrue(store.getPolicy("orders").isEmpty());
        assertEquals(1, store.casPolicy("orders", 0, policy("orders", 5, 0)), "create at rev 0 -> 1");
        assertEquals(-1, store.casPolicy("orders", 0, policy("orders", 5, 0)), "second create loses");
        assertEquals(2, store.casPolicy("orders", 1, policy("orders", 6, 0)), "update at rev 1 -> 2");
        assertEquals(-1, store.casPolicy("orders", 1, policy("orders", 7, 0)), "stale revision loses");

        CoordPolicy stored = store.getPolicy("orders").orElseThrow();
        assertEquals(2, stored.revision());
        assertEquals(6, stored.currentEpoch());
        Ring.Epoch ring = stored.epochs().get(6L);
        assertEquals(Ring.Status.OPEN, ring.status());
        assertEquals("cellA", ring.ring().get(0).cellId());
        assertEquals("us", ring.ring().get(1).region());
        assertEquals(1, store.listPolicies().size());
    }

    @Test @DisplayName("node roster: upsert inserts then updates, filter by namespace, touch, remove, expire")
    void nodeRoster() {
        CoordNode n1 = new CoordNode("n1", "orders", "cellA", "10.0.0.1:8080", "eu", "1.0", "fpA", 0, 100);
        store.upsertNode(n1);
        assertEquals("10.0.0.1:8080", store.node("n1").orElseThrow().endpoint());

        store.upsertNode(new CoordNode("n1", "orders", "cellA", "10.0.0.9:8080", "eu", "1.0", "fpA", 3, 150));
        assertEquals("10.0.0.9:8080", store.node("n1").orElseThrow().endpoint(), "upsert updated in place");
        assertEquals(3, store.node("n1").orElseThrow().configGeneration());

        store.upsertNode(new CoordNode("n2", "billing", "cellX", "10.0.0.2:8080", "us", "1.0", "fpX", 0, 100));
        assertEquals(1, store.nodes("orders").size());
        assertEquals(1, store.nodes("billing").size());

        assertTrue(store.touchNode("n1", 500, 7).isPresent());
        assertEquals(500, store.node("n1").orElseThrow().lastHeartbeat());
        assertEquals(7, store.node("n1").orElseThrow().configGeneration());
        assertTrue(store.touchNode("ghost", 1, 1).isEmpty());

        store.removeNode("n2");
        assertTrue(store.node("n2").isEmpty());

        // n1's heartbeat is now 500; add a stale node and expire everything older than 400.
        store.upsertNode(new CoordNode("n3", "orders", "cellA", "e", "eu", "1.0", "fpA", 0, 50));
        assertEquals(1, store.expireNodes(400), "only the stale n3 (hb 50 < 400) is removed");
        assertTrue(store.node("n1").isPresent(), "n1 (hb 500) survives");
        assertTrue(store.node("n3").isEmpty());
    }

    @Test @DisplayName("cell binding is an atomic claim; a different fingerprint is rejected")
    void cellBinding() {
        assertTrue(store.bindCell("orders", "cellA", "fp-1"), "first claim");
        assertTrue(store.bindCell("orders", "cellA", "fp-1"), "same cell re-registers");
        assertFalse(store.bindCell("orders", "cellA", "fp-2"), "a different cell can't steal the id");
        assertTrue(store.bindCell("orders", "cellA", null), "null fingerprint skips the guard");

        // orphan prune: a binding with no live node is removed; one referenced by a node stays.
        store.upsertNode(new CoordNode("n1", "orders", "cellA", "e", "eu", "1.0", "fp-1", 0, 1));
        store.bindCell("orders", "cellGone", "fp-gone");
        assertEquals(1, store.pruneOrphanCellBindings(), "only cellGone is orphaned");
        assertFalse(store.bindCell("orders", "cellA", "fp-2"), "cellA binding still held");
        assertTrue(store.bindCell("orders", "cellGone", "fp-new"), "cellGone freed, reusable");
    }

    @Test @DisplayName("definition registry: idempotent upsert, list, remove")
    void definitions() {
        store.putDefinition(new CoordDefinition("orders", "checkout", 1, "h1", 100));
        assertEquals(1, store.getDefinition("orders", "checkout").orElseThrow().version());
        store.putDefinition(new CoordDefinition("orders", "checkout", 2, "h2", 200));
        assertEquals(2, store.getDefinition("orders", "checkout").orElseThrow().version(), "upserted");
        store.putDefinition(new CoordDefinition("orders", "refund", 1, "h3", 300));
        assertEquals(2, store.definitions("orders").size());
        assertTrue(store.removeDefinition("orders", "refund"));
        assertFalse(store.removeDefinition("orders", "refund"), "second remove is a no-op");
        assertEquals(1, store.definitions("orders").size());
    }

    @Test @DisplayName("namespace registry round-trips, including the storage config")
    void namespaceRegistry() {
        StorageConfig sc = StorageConfig.jdbc("jdbc:postgresql://db/orders", "wiggle", "secret://pw", 8);
        CoordNamespace ns = new CoordNamespace("orders", ProvisionState.REQUESTED, sc, 3, "eu", null, null, 100);
        store.putNamespace(ns);

        CoordNamespace got = store.getNamespace("orders").orElseThrow();
        assertEquals(ProvisionState.REQUESTED, got.state());
        assertEquals("jdbc:postgresql://db/orders", got.storage().jdbcUrl());
        assertEquals("wiggle", got.storage().user());
        assertEquals("secret://pw", got.storage().secretRef());
        assertEquals(8, got.storage().poolSize());
        assertEquals(3, got.replicas());
        assertEquals("eu", got.region());
        assertNull(got.endpoint());

        store.putNamespace(got.active("10.0.0.5:8080", 200));
        CoordNamespace active = store.getNamespace("orders").orElseThrow();
        assertEquals(ProvisionState.ACTIVE, active.state());
        assertEquals("10.0.0.5:8080", active.endpoint());
        assertEquals(1, store.namespaces().size());
    }

    @Test @DisplayName("election roster: announce, heartbeat, seniority, takeover on expiry, stand-down")
    void electionRoster() {
        ElectionStore election = store.election();
        long t0 = 100_000;
        long deadAfter = 5_000;

        // Two coordinators announce. A is older, so A leads -- and both compute that independently,
        // which is the property the whole scheme rests on.
        Member a = new Member("A", "A", t0, t0);
        Member b = new Member("B", "B", t0 + 500, t0 + 500);
        election.step(a, 0, roster -> null);
        election.step(b, 0, roster -> null);

        List<Member> roster = election.members();
        assertEquals(2, roster.size(), "both are on the roster");
        assertEquals("A", LeaderElection.electedLeader(roster, t0 + 600, deadAfter), "the older one leads");

        // A keeps beating; B beating too does not move leadership, because seniority is first_heartbeat
        // and that must not move on a heartbeat.
        election.step(a.withHeartbeat(t0 + 1_000), 0, r -> null);
        election.step(b.withHeartbeat(t0 + 1_000), 0, r -> null);
        assertEquals("A", LeaderElection.electedLeader(election.members(), t0 + 1_100, deadAfter),
                "a heartbeat must not reset seniority");

        // A goes quiet: once its last heartbeat falls outside the window, B takes over.
        election.step(b.withHeartbeat(t0 + 9_000), 0, r -> null);
        assertEquals("B", LeaderElection.electedLeader(election.members(), t0 + 9_100, deadAfter),
                "B leads once A's heartbeat went stale");

        // A comes back and is senior again -- it never lost its first_heartbeat.
        election.step(a.withHeartbeat(t0 + 9_200), 0, r -> null);
        assertEquals("A", LeaderElection.electedLeader(election.members(), t0 + 9_300, deadAfter),
                "a returning node resumes its seniority");

        // Standing down backdates the heartbeat, so peers re-elect at once instead of waiting the window.
        election.standDown(a);
        assertEquals("B", LeaderElection.electedLeader(election.members(), t0 + 9_400, deadAfter),
                "stand-down hands over immediately");

        // Pruning drops only the long dead.
        election.step(b.withHeartbeat(t0 + 20_000), t0 + 10_000, r -> null);
        assertEquals(List.of("B"), election.members().stream().map(Member::id).toList(),
                "A's backdated row is pruned, B's is not");
    }
}
