package com.wiggle.coordinator.ratis;

import com.wiggle.server.coord.CoordDefinition;
import com.wiggle.server.coord.CoordNamespace;
import com.wiggle.server.coord.CoordNode;
import com.wiggle.server.coord.CoordPolicy;
import com.wiggle.server.coord.CoordPolicy.EpochRing;
import com.wiggle.server.coord.CoordPolicy.EpochStatus;
import com.wiggle.server.coord.CoordPolicy.RingSlot;
import com.wiggle.server.coord.CoordinatorStore;
import com.wiggle.server.coord.NamespaceSpec;
import com.wiggle.server.coord.ProvisionState;
import com.wiggle.server.coord.StorageConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The {@link CoordinatorStore} contract, exercised against a real embedded Ratis+RocksDB store: each test
 * boots a single-member Raft group in a temp directory (its own free port + storage), runs the same
 * scenarios the JDBC/in-memory stores are held to (see {@code com.wiggle.postgres.CoordinatorStoreTest}),
 * and proves the full write→Raft-log→apply→RocksDB→reply round-trip end to end. A per-test timeout guards
 * against a group that never elects a leader.
 */
class RatisCoordinatorStoreTest {

    private static CoordPolicy policy(String ns, long currentEpoch, Map<Long, EpochRing> epochs) {
        return new CoordPolicy(ns, currentEpoch, 0 /* revision ignored on write */, epochs);
    }

    private static EpochRing ring(String cell, EpochStatus status) {
        return new EpochRing(List.of(new RingSlot(0, cell, "eu-west")), status);
    }

    /** Boot a single-member Ratis coordinator store rooted at {@code dir} on an ephemeral port. */
    private static CoordinatorStore bootStore(Path dir) throws IOException {
        int port;
        try (ServerSocket s = new ServerSocket(0)) { port = s.getLocalPort(); }
        String uri = "ratis://" + dir + "?peers=n0@127.0.0.1:" + port;
        return new RatisCoordinatorStoreProvider(uri).coordinatorStore();
    }

    @Test @DisplayName("embedded Ratis store honours the CoordinatorStore contract")
    @Timeout(60)
    void contract(@TempDir Path dir) throws Exception {
        try (CoordinatorStore store = bootStore(dir)) {
            // ---- policy CAS ----
            assertTrue(store.getPolicy("acme").isEmpty());
            long r1 = store.casPolicy("acme", 0, policy("acme", 0, Map.of(0L, ring("cell-3", EpochStatus.OPEN))));
            assertEquals(1, r1, "create returns revision 1");
            assertEquals(-1, store.casPolicy("acme", 0, policy("acme", 0, Map.of(0L, ring("cell-3", EpochStatus.OPEN)))),
                    "a second create loses");

            CoordPolicy got = store.getPolicy("acme").orElseThrow();
            assertEquals(1, got.revision());
            assertEquals(0, got.currentEpoch());
            assertEquals("cell-3", got.epochs().get(0L).ring().get(0).cellId());
            assertEquals(EpochStatus.OPEN, got.epochs().get(0L).status());

            // scale out: open epoch 1, drain epoch 0 -- CAS on revision 1
            Map<Long, EpochRing> two = Map.of(
                    0L, ring("cell-3", EpochStatus.DRAINING),
                    1L, ring("cell-5", EpochStatus.OPEN));
            assertEquals(2, store.casPolicy("acme", 1, policy("acme", 1, two)));
            assertEquals(-1, store.casPolicy("acme", 1, policy("acme", 1, two)), "a stale-revision update loses");

            CoordPolicy got2 = store.getPolicy("acme").orElseThrow();
            assertEquals(2, got2.revision());
            assertEquals(1, got2.currentEpoch());
            assertEquals(2, got2.epochs().size());
            assertEquals(EpochStatus.DRAINING, got2.epochs().get(0L).status());
            assertEquals(1, store.listPolicies().size());

            // ---- node roster ----
            store.upsertNode(new CoordNode("n1", "acme", "cellA", "grpc://h:1", "eu-west", "2.1.5", "fp-A", 2, 1_000));
            store.upsertNode(new CoordNode("n1", "acme", "cellA", "grpc://h:2", "eu-west", "2.1.5", "fp-A", 2, 2_000)); // upsert same id
            assertEquals(1, store.nodes("acme").size());
            assertEquals("grpc://h:2", store.nodes("acme").get(0).endpoint(), "upsert replaced the endpoint");
            assertEquals("cellA", store.nodes("acme").get(0).cellId(), "cell id round-trips");
            assertEquals("fp-A", store.nodes("acme").get(0).cellFingerprint(), "cell fingerprint round-trips");
            assertEquals("grpc://h:2", store.node("n1").orElseThrow().endpoint(), "node(id) round-trips");

            // touchNode updates heartbeat + generation and returns the updated node
            CoordNode touched = store.touchNode("n1", 5_000, 7).orElseThrow();
            assertEquals(5_000, touched.lastHeartbeat());
            assertEquals(7, touched.configGeneration());
            assertTrue(store.touchNode("missing", 1, 1).isEmpty(), "touching an unknown node is empty");

            store.upsertNode(new CoordNode("n2", "acme", "cellB", "grpc://h:3", "eu-west", "2.1.5", "fp-A", 2, 500));
            assertEquals(2, store.nodes("acme").size());
            assertEquals(1, store.expireNodes(900), "n2 (hb 500) is stale");
            assertEquals(1, store.nodes("acme").size());
            store.removeNode("n1");
            assertEquals(0, store.nodes("acme").size(), "removeNode drops the last node");

            // ---- definitions (idempotent) ----
            assertTrue(store.getDefinition("acme", "order").isEmpty());
            store.putDefinition(new CoordDefinition("acme", "order", 42, "hashA", 111));
            store.putDefinition(new CoordDefinition("acme", "order", 42, "hashA", 111)); // idempotent
            assertEquals(1, store.definitions("acme").size());
            assertEquals("hashA", store.getDefinition("acme", "order").orElseThrow().hash());

            store.putDefinition(new CoordDefinition("acme", "order", 43, "hashB", 222)); // update in place
            assertEquals(43, store.getDefinition("acme", "order").orElseThrow().version());
            assertEquals(1, store.definitions("acme").size());
            assertTrue(store.removeDefinition("acme", "order"), "removing an existing definition reports true");
            assertFalse(store.removeDefinition("acme", "order"), "removing a missing definition reports false");
            assertTrue(store.definitions("acme").isEmpty());

            // ---- namespace registry (provisioning, T13) ----
            assertTrue(store.getNamespace("acme").isEmpty());
            StorageConfig sc = StorageConfig.jdbc("jdbc:postgresql://db/acme", "app", "ACME_DB_SECRET", 8);
            store.putNamespace(CoordNamespace.requested(new NamespaceSpec("acme", sc, 2, "eu-west", 8100), 1_000));
            CoordNamespace ns = store.getNamespace("acme").orElseThrow();
            assertEquals(ProvisionState.REQUESTED, ns.state());
            assertEquals("ACME_DB_SECRET", ns.storage().secretRef(), "the ref is stored, never the secret");
            assertEquals(2, ns.replicas());

            store.putNamespace(ns.active("grpc://acme:8100", 2_000));   // upsert in place
            CoordNamespace active = store.getNamespace("acme").orElseThrow();
            assertEquals(ProvisionState.ACTIVE, active.state());
            assertEquals("grpc://acme:8100", active.endpoint());
            assertEquals(1, store.namespaces().size());
        }
    }

    @Test @DisplayName("embedded Ratis store: atomic cell-identity binding + orphan pruning")
    @Timeout(60)
    void cellBinding(@TempDir Path dir) throws Exception {
        try (CoordinatorStore store = bootStore(dir)) {
            assertTrue(store.bindCell("orders", "cellA", "fp-A"), "first cell claims the id");
            assertTrue(store.bindCell("orders", "cellA", "fp-A"), "a replica of the same cell matches");
            assertFalse(store.bindCell("orders", "cellA", "fp-B"), "a different cell may not reuse the id");
            assertTrue(store.bindCell("orders", "cellB", "fp-B"), "a different cell id is free");
            assertTrue(store.bindCell("other", "cellA", "fp-B"), "the same id in another namespace is independent");
            assertTrue(store.bindCell("orders", "cellA", null), "a null fingerprint skips the guard");

            // No node references any binding yet -> all three are orphans and prune reclaims them.
            assertEquals(3, store.pruneOrphanCellBindings(), "orphan bindings pruned");
            assertTrue(store.bindCell("orders", "cellA", "fp-C"), "a pruned id is reusable by a new cell");

            // A live node keeps its cell's binding.
            store.upsertNode(new CoordNode("bn1", "orders", "cellA", "grpc://h:1", "eu", "v", "fp-C", 0, 1_000));
            assertEquals(0, store.pruneOrphanCellBindings(), "a binding with a live node is kept");
        }
    }

    @Test @DisplayName("embedded Ratis store: a multi-slot ring round-trips every slot")
    @Timeout(60)
    void multiSlotRingRoundTrips(@TempDir Path dir) throws Exception {
        try (CoordinatorStore store = bootStore(dir)) {
            // epoch 1 with TWO slots: shard 0 -> cellA, shard 1 -> cellB (the reshard the lab does).
            EpochRing two = new EpochRing(List.of(
                    new RingSlot(0, "cellA", "eu"),
                    new RingSlot(1, "cellB", "us")), EpochStatus.OPEN);
            assertEquals(1, store.casPolicy("abc", 0, policy("abc", 1, Map.of(1L, two))));

            CoordPolicy got = store.getPolicy("abc").orElseThrow();
            EpochRing er = got.epochs().get(1L);
            assertEquals(2, er.ring().size(), "both ring slots persist");
            assertEquals("cellA", er.ring().get(0).cellId());
            assertEquals(0, er.ring().get(0).shard());
            assertEquals("cellB", er.ring().get(1).cellId());
            assertEquals(1, er.ring().get(1).shard());
        }
    }

    @Test @DisplayName("embedded Ratis store: leadership lease is take-once, renewable, and releasable")
    @Timeout(60)
    void leadership(@TempDir Path dir) throws Exception {
        try (CoordinatorStore store = bootStore(dir)) {
            long lease = 10_000;
            assertTrue(store.acquireLeadership("A", 1_000, lease), "A claims the free lease");   // A expires at 11_000
            assertTrue(store.acquireLeadership("A", 2_000, lease), "A renews its own lease");    // renew -> expires at 12_000
            assertFalse(store.acquireLeadership("B", 3_000, lease), "B cannot take a valid lease");
            assertTrue(store.acquireLeadership("B", 12_000, lease), "B takes over the moment A's lease expires"); // B expires at 22_000
            assertFalse(store.acquireLeadership("A", 13_000, lease), "A now blocked by B's valid lease");
            store.releaseLeadership("B");
            assertTrue(store.acquireLeadership("A", 14_000, lease), "a released lease is free again");
        }
    }
}