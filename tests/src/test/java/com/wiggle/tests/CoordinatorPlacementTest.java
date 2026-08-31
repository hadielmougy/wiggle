package com.wiggle.tests;

import com.wiggle.core.Tls;
import com.wiggle.proto.NodeConfig;
import com.wiggle.proto.RegisterResponse;
import com.wiggle.proto.RegisteredNode;
import com.wiggle.proto.RingSlot;
import com.wiggle.server.coord.CoordinatorApi;
import com.wiggle.server.coord.InMemoryCoordinatorStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T12 increment 2: registration and FetchConfig hand a node its placement -- the epoch it mints into
 * and the shards its cell owns in that epoch's ring -- so a cell only stamps shards that resolve back
 * to itself.
 */
class CoordinatorPlacementTest {

    private static RegisteredNode node(String endpoint, String cellId) {
        return RegisteredNode.newBuilder().setName(endpoint).setEndpoint(endpoint).setCellId(cellId).build();
    }

    private static RegisteredNode node(String endpoint, String cellId, String fingerprint) {
        return RegisteredNode.newBuilder().setName(endpoint).setEndpoint(endpoint)
                .setCellId(cellId).setCellFingerprint(fingerprint).build();
    }

    @Test @DisplayName("with no ring, a node is placed at epoch 0 shard 0 (single implicit cell)")
    void singleCellDefault() throws Exception {
        try (CoordinatorApi api = new CoordinatorApi(new InMemoryCoordinatorStore(), 0, Tls.Options.DISABLED)) {
            RegisterResponse r = api.service().doRegister("orders", node("grpc://h:1", ""));
            assertEquals(0, r.getEpoch());
            assertEquals(List.of(0), r.getShardsList());

            NodeConfig cfg = api.service().doFetchConfig("orders");
            assertEquals(0, cfg.getEpoch());
            assertEquals(List.of(0), cfg.getShardsList());
        }
    }

    @Test @DisplayName("a node is handed exactly the shards its cell owns in the current epoch")
    void ownedShardsPerCell() throws Exception {
        try (CoordinatorApi api = new CoordinatorApi(new InMemoryCoordinatorStore(), 0, Tls.Options.DISABLED)) {
            // ring: shard 0,1 -> cellA ; shard 2 -> cellB
            api.service().doRegister("orders", node("grpc://a:1", "cellA"));
            api.service().doRegister("orders", node("grpc://b:1", "cellB"));
            api.service().doOpenEpoch("orders", List.of(
                    RingSlot.newBuilder().setShard(0).setCellId("cellA").build(),
                    RingSlot.newBuilder().setShard(1).setCellId("cellA").build(),
                    RingSlot.newBuilder().setShard(2).setCellId("cellB").build()));

            NodeConfig a = api.service().doFetchConfig("orders", "cellA");
            assertEquals(0, a.getEpoch());
            assertEquals(List.of(0, 1), a.getShardsList(), "cellA owns shards 0 and 1");

            NodeConfig b = api.service().doFetchConfig("orders", "cellB");
            assertEquals(List.of(2), b.getShardsList(), "cellB owns shard 2");
        }
    }

    @Test @DisplayName("registration after an epoch bump places the node in the new (current) epoch")
    void placementFollowsCurrentEpoch() throws Exception {
        try (CoordinatorApi api = new CoordinatorApi(new InMemoryCoordinatorStore(), 0, Tls.Options.DISABLED)) {
            api.service().doOpenEpoch("orders", List.of(RingSlot.newBuilder().setShard(0).setCellId("cellA").build())); // epoch 0
            api.service().doOpenEpoch("orders", List.of(RingSlot.newBuilder().setShard(0).setCellId("cellA").build())); // epoch 1

            RegisterResponse r = api.service().doRegister("orders", node("grpc://a:1", "cellA"));
            assertEquals(1, r.getEpoch(), "new nodes mint into the current epoch, not the draining one");
            assertEquals(List.of(0), r.getShardsList());
        }
    }

    @Test @DisplayName("a second cell reusing a cell id in the same namespace is rejected")
    void rejectsDuplicateCellId() throws Exception {
        try (CoordinatorApi api = new CoordinatorApi(new InMemoryCoordinatorStore(), 0, Tls.Options.DISABLED)) {
            api.service().doRegister("orders", node("grpc://a:1", "cellX", "fp-DB-A"));   // establishes the binding
            api.service().doRegister("orders", node("grpc://b:1", "cellX", "fp-DB-A"));   // same cell replica -> ok

            assertThrows(IllegalArgumentException.class,
                    () -> api.service().doRegister("orders", node("grpc://c:1", "cellX", "fp-DB-B")),
                    "a node from a different cell (distinct fingerprint) may not reuse the cell id");

            // A different cell id from the second storage is fine; and the same id in another namespace is independent.
            api.service().doRegister("orders", node("grpc://c:1", "cellY", "fp-DB-B"));
            api.service().doRegister("billing", node("grpc://c:1", "cellX", "fp-DB-B"));
        }
    }

    @Test @DisplayName("with no ring, a stray extra cell is standby while the implicit cell mints (no hard reject)")
    void noRingExtraCellIsStandby() throws Exception {
        try (CoordinatorApi api = new CoordinatorApi(new InMemoryCoordinatorStore(), 0, Tls.Options.DISABLED)) {
            api.service().doRegister("orders", node("grpc://a:1", "orders"));   // implicit cell (id == namespace)
            api.service().doRegister("orders", node("grpc://b:1", "cellB"));    // extra cell, no epoch opened -- admitted, not rejected

            assertEquals(List.of(0), api.service().doFetchConfig("orders", "orders").getShardsList(),
                    "the implicit cell keeps minting genesis");
            assertTrue(api.service().doFetchConfig("orders", "cellB").getShardsList().isEmpty(),
                    "the extra cell is placed on standby (no shards), so it cannot forge mis-routing ids");
        }
    }

    @Test @DisplayName("a cell not named in the ring is placed on standby (empty shards), not genesis (guard #2)")
    void unringedCellIsStandby() throws Exception {
        try (CoordinatorApi api = new CoordinatorApi(new InMemoryCoordinatorStore(), 0, Tls.Options.DISABLED)) {
            api.service().doRegister("orders", node("grpc://a:1", "cellA"));
            api.service().doRegister("orders", node("grpc://b:1", "cellB"));
            // ring names ONLY cellA; cellB is deliberately unplaced
            api.service().doOpenEpoch("orders", List.of(RingSlot.newBuilder().setShard(0).setCellId("cellA").build()));

            assertEquals(List.of(0), api.service().doFetchConfig("orders", "cellA").getShardsList(), "cellA owns shard 0");
            assertTrue(api.service().doFetchConfig("orders", "cellB").getShardsList().isEmpty(),
                    "cellB is not in the ring -> standby (no shards, so the node refuses to mint)");
        }
    }

    @Test @DisplayName("a node without a fingerprint never trips the guard (in-memory / legacy)")
    void noFingerprintSkipsGuard() throws Exception {
        try (CoordinatorApi api = new CoordinatorApi(new InMemoryCoordinatorStore(), 0, Tls.Options.DISABLED)) {
            api.service().doRegister("orders", node("grpc://a:1", "cellX"));   // no fingerprint
            api.service().doRegister("orders", node("grpc://b:1", "cellX"));   // still no fingerprint -> allowed
        }
    }
}
