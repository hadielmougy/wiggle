package com.wiggle.tests;

import com.wiggle.core.IdCodec;
import com.wiggle.core.Ids;
import com.wiggle.core.Tls;
import com.wiggle.proto.ActiveCellsResponse;
import com.wiggle.proto.RegisteredNode;
import com.wiggle.proto.ResolveRequest;
import com.wiggle.proto.ResolveResponse;
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
 * T12 (addressing prerequisite): when a namespace spans several cells, an instance id resolves to the
 * cell that owns its shard -- so routing is directory-free and shard-correct. The ring maps
 * {@code shard -> cellId} and each node registers its {@code cellId}; Resolve intersects the two.
 */
class MultiCellResolveTest {

    private static RegisteredNode node(String endpoint, String cellId) {
        return RegisteredNode.newBuilder().setName(endpoint).setEndpoint(endpoint).setCellId(cellId).build();
    }

    /** namespace "orders" over two cells: shard 0 -> cellA, shard 1 -> cellB. */
    private static void twoCellNamespace(CoordinatorApi api) {
        api.doRegister("orders", node("grpc://a1:1", "cellA"));
        api.doRegister("orders", node("grpc://a2:1", "cellA"));
        api.doRegister("orders", node("grpc://b1:1", "cellB"));
        api.doOpenEpoch("orders", List.of(
                RingSlot.newBuilder().setShard(0).setCellId("cellA").build(),
                RingSlot.newBuilder().setShard(1).setCellId("cellB").build()));
    }

    @Test @DisplayName("an instance id resolves to the cell that owns its shard")
    void resolveByShard() throws Exception {
        InMemoryCoordinatorStore store = new InMemoryCoordinatorStore();
        try (CoordinatorApi api = new CoordinatorApi(store, 0, Tls.Options.DISABLED)) {
            twoCellNamespace(api);

            String s0 = IdCodec.format("orders", 0, 0, Ids.token());
            String s1 = IdCodec.format("orders", 0, 1, Ids.token());

            ResolveResponse r0 = api.doResolve(ResolveRequest.newBuilder().setInstanceId(s0).build());
            assertTrue(r0.getEndpoint().getAddressesList().containsAll(List.of("grpc://a1:1", "grpc://a2:1")),
                    "shard 0 -> cellA nodes");
            assertEquals(2, r0.getEndpoint().getAddressesList().size(), "only cellA nodes");

            ResolveResponse r1 = api.doResolve(ResolveRequest.newBuilder().setInstanceId(s1).build());
            assertEquals(List.of("grpc://b1:1"), r1.getEndpoint().getAddressesList(), "shard 1 -> cellB node");
        }
    }

    @Test @DisplayName("a shard beyond the ring wraps by modulo")
    void resolveShardBeyondRing() throws Exception {
        InMemoryCoordinatorStore store = new InMemoryCoordinatorStore();
        try (CoordinatorApi api = new CoordinatorApi(store, 0, Tls.Options.DISABLED)) {
            twoCellNamespace(api);
            String s2 = IdCodec.format("orders", 0, 2, Ids.token());   // 2 % 2 == 0 -> cellA
            ResolveResponse r = api.doResolve(ResolveRequest.newBuilder().setInstanceId(s2).build());
            assertEquals(2, r.getEndpoint().getAddressesList().size(), "wraps to cellA");
        }
    }

    @Test @DisplayName("activeCells lists every cell in the ring so a worker polls both")
    void activeCellsSpansCells() throws Exception {
        InMemoryCoordinatorStore store = new InMemoryCoordinatorStore();
        try (CoordinatorApi api = new CoordinatorApi(store, 0, Tls.Options.DISABLED)) {
            twoCellNamespace(api);
            ActiveCellsResponse ac = api.doActiveCells("orders", null);
            assertEquals(2, ac.getCellsList().size(), "both cells host live work");
            List<String> targets = ac.getCellsList().stream().map(e -> e.getTarget()).sorted().toList();
            assertTrue(targets.get(0).startsWith("grpc://a"), "one endpoint is cellA");
            assertTrue(targets.get(1).startsWith("grpc://b"), "the other is cellB");
        }
    }

    @Test @DisplayName("resolving to a cell with no live nodes fails (not a silent wrong-cell fallback)")
    void emptyCellFails() throws Exception {
        InMemoryCoordinatorStore store = new InMemoryCoordinatorStore();
        try (CoordinatorApi api = new CoordinatorApi(store, 0, Tls.Options.DISABLED)) {
            api.doRegister("orders", node("grpc://a1:1", "cellA"));
            api.doOpenEpoch("orders", List.of(
                    RingSlot.newBuilder().setShard(0).setCellId("cellA").build(),
                    RingSlot.newBuilder().setShard(1).setCellId("cellB").build()));   // cellB has no nodes
            String s1 = IdCodec.format("orders", 0, 1, Ids.token());
            assertThrows(IllegalStateException.class,
                    () -> api.doResolve(ResolveRequest.newBuilder().setInstanceId(s1).build()));
        }
    }
}
