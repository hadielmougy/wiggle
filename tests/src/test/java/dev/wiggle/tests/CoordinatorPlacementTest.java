package dev.wiggle.tests;

import dev.wiggle.core.Tls;
import dev.wiggle.proto.NodeConfig;
import dev.wiggle.proto.RegisterResponse;
import dev.wiggle.proto.RegisteredNode;
import dev.wiggle.proto.RingSlot;
import dev.wiggle.server.coord.CoordinatorApi;
import dev.wiggle.server.coord.InMemoryCoordinatorStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * T12 increment 2: registration and FetchConfig hand a node its placement -- the epoch it mints into
 * and the shards its cell owns in that epoch's ring -- so a cell only stamps shards that resolve back
 * to itself.
 */
class CoordinatorPlacementTest {

    private static RegisteredNode node(String endpoint, String cellId) {
        return RegisteredNode.newBuilder().setName(endpoint).setEndpoint(endpoint).setCellId(cellId).build();
    }

    @Test @DisplayName("with no ring, a node is placed at epoch 0 shard 0 (single implicit cell)")
    void singleCellDefault() throws Exception {
        try (CoordinatorApi api = new CoordinatorApi(new InMemoryCoordinatorStore(), 0, Tls.Options.DISABLED)) {
            RegisterResponse r = api.doRegister("orders", node("grpc://h:1", ""));
            assertEquals(0, r.getEpoch());
            assertEquals(List.of(0), r.getShardsList());

            NodeConfig cfg = api.doFetchConfig("orders");
            assertEquals(0, cfg.getEpoch());
            assertEquals(List.of(0), cfg.getShardsList());
        }
    }

    @Test @DisplayName("a node is handed exactly the shards its cell owns in the current epoch")
    void ownedShardsPerCell() throws Exception {
        try (CoordinatorApi api = new CoordinatorApi(new InMemoryCoordinatorStore(), 0, Tls.Options.DISABLED)) {
            // ring: shard 0,1 -> cellA ; shard 2 -> cellB
            api.doRegister("orders", node("grpc://a:1", "cellA"));
            api.doRegister("orders", node("grpc://b:1", "cellB"));
            api.doOpenEpoch("orders", List.of(
                    RingSlot.newBuilder().setShard(0).setCellId("cellA").build(),
                    RingSlot.newBuilder().setShard(1).setCellId("cellA").build(),
                    RingSlot.newBuilder().setShard(2).setCellId("cellB").build()));

            NodeConfig a = api.doFetchConfig("orders", "cellA");
            assertEquals(0, a.getEpoch());
            assertEquals(List.of(0, 1), a.getShardsList(), "cellA owns shards 0 and 1");

            NodeConfig b = api.doFetchConfig("orders", "cellB");
            assertEquals(List.of(2), b.getShardsList(), "cellB owns shard 2");
        }
    }

    @Test @DisplayName("registration after an epoch bump places the node in the new (current) epoch")
    void placementFollowsCurrentEpoch() throws Exception {
        try (CoordinatorApi api = new CoordinatorApi(new InMemoryCoordinatorStore(), 0, Tls.Options.DISABLED)) {
            api.doOpenEpoch("orders", List.of(RingSlot.newBuilder().setShard(0).setCellId("cellA").build())); // epoch 0
            api.doOpenEpoch("orders", List.of(RingSlot.newBuilder().setShard(0).setCellId("cellA").build())); // epoch 1

            RegisterResponse r = api.doRegister("orders", node("grpc://a:1", "cellA"));
            assertEquals(1, r.getEpoch(), "new nodes mint into the current epoch, not the draining one");
            assertEquals(List.of(0), r.getShardsList());
        }
    }
}
