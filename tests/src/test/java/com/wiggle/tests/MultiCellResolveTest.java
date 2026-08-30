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
import static org.junit.jupiter.api.Assertions.assertFalse;
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

    /** namespace "orders" over three cells: shard 0 -> cellA, 1 -> cellB, 2 -> cellC. */
    private static void threeCellNamespace(CoordinatorApi api) {
        api.doRegister("orders", node("grpc://a1:1", "cellA"));
        api.doRegister("orders", node("grpc://a2:1", "cellA"));
        api.doRegister("orders", node("grpc://b1:1", "cellB"));
        api.doRegister("orders", node("grpc://c1:1", "cellC"));
        api.doOpenEpoch("orders", List.of(
                RingSlot.newBuilder().setShard(0).setCellId("cellA").build(),
                RingSlot.newBuilder().setShard(1).setCellId("cellB").build(),
                RingSlot.newBuilder().setShard(2).setCellId("cellC").build()));
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

    @Test @DisplayName("adding a shard to an OPEN epoch via SetRing keeps existing ids and routes the new shard")
    void addShardInPlace() throws Exception {
        InMemoryCoordinatorStore store = new InMemoryCoordinatorStore();
        try (CoordinatorApi api = new CoordinatorApi(store, 0, Tls.Options.DISABLED)) {
            twoCellNamespace(api);                                   // epoch 0: shard 0 -> cellA, 1 -> cellB
            api.doRegister("orders", node("grpc://c1:1", "cellC"));  // the cell the new shard will land on

            String s0 = IdCodec.format("orders", 0, 0, Ids.token());
            String s1 = IdCodec.format("orders", 0, 1, Ids.token());
            String s2 = IdCodec.format("orders", 0, 2, Ids.token());

            // Before: shard 2 is not in the ring, so it wraps by modulo (2 % 2 == 0 -> cellA).
            assertEquals(2, api.doResolve(ResolveRequest.newBuilder().setInstanceId(s2).build())
                    .getEndpoint().getAddressesList().size(), "unmapped shard wraps to cellA");
            var before = store.getPolicy("orders").orElseThrow();
            assertEquals(0, before.currentEpoch());
            assertEquals(1, before.epochs().size());

            // Add shard 2 -> cellC IN PLACE, on the current (OPEN) epoch.
            api.doSetRing("orders", 0, List.of(
                    RingSlot.newBuilder().setShard(0).setCellId("cellA").build(),
                    RingSlot.newBuilder().setShard(1).setCellId("cellB").build(),
                    RingSlot.newBuilder().setShard(2).setCellId("cellC").build()));

            var after = store.getPolicy("orders").orElseThrow();
            assertEquals(0, after.currentEpoch(), "no new epoch -- currentEpoch unchanged");
            assertEquals(1, after.epochs().size(), "no new epoch created");
            assertTrue(after.revision() > before.revision(), "generation bumps so nodes re-fetch placement");

            // Existing ids are untouched -- the additive edit moved no live instance.
            assertTrue(api.doResolve(ResolveRequest.newBuilder().setInstanceId(s0).build())
                    .getEndpoint().getAddressesList().containsAll(List.of("grpc://a1:1", "grpc://a2:1")), "s0 still cellA");
            assertEquals(List.of("grpc://b1:1"), api.doResolve(ResolveRequest.newBuilder().setInstanceId(s1).build())
                    .getEndpoint().getAddressesList(), "s1 still cellB");

            // The added shard now routes to its cell, and cellC is handed shard 2 to mint into.
            assertEquals(List.of("grpc://c1:1"), api.doResolve(ResolveRequest.newBuilder().setInstanceId(s2).build())
                    .getEndpoint().getAddressesList(), "s2 now resolves to cellC");
            assertEquals(List.of(2), api.doFetchConfig("orders", "cellC").getShardsList(),
                    "cellC now owns shard 2 for new mints");
        }
    }

    @Test @DisplayName("no-ring resolve routes to the implicit cell and ignores a stray extra cell (no outage)")
    void noRingRoutesToImplicitCell() throws Exception {
        InMemoryCoordinatorStore store = new InMemoryCoordinatorStore();
        try (CoordinatorApi api = new CoordinatorApi(store, 0, Tls.Options.DISABLED)) {
            api.doRegister("orders", node("grpc://a1:1", "orders"));   // the implicit cell (id == namespace)
            api.doRegister("orders", node("grpc://b1:1", "cellB"));     // a stray extra cell; no epoch opened

            ResolveResponse r = api.doResolve(ResolveRequest.newBuilder().setNamespace("orders").build());
            assertEquals(List.of("grpc://a1:1"), r.getEndpoint().getAddressesList(),
                    "routes to the implicit cell 'orders', ignoring the stray cellB -- no outage, no pooling");
        }
    }

    @Test @DisplayName("removing a shard IN PLACE silently mis-routes an existing instance on it (unsafe)")
    void removeShardInPlaceMisroutes() throws Exception {
        InMemoryCoordinatorStore store = new InMemoryCoordinatorStore();
        try (CoordinatorApi api = new CoordinatorApi(store, 0, Tls.Options.DISABLED)) {
            threeCellNamespace(api);                                  // epoch 0: 0->cellA, 1->cellB, 2->cellC
            String s2 = IdCodec.format("orders", 0, 2, Ids.token());
            assertEquals(List.of("grpc://c1:1"), api.doResolve(ResolveRequest.newBuilder().setInstanceId(s2).build())
                    .getEndpoint().getAddressesList(), "s2 starts on cellC");

            // Remove shard 2 from the current epoch in place.
            api.doSetRing("orders", 0, List.of(
                    RingSlot.newBuilder().setShard(0).setCellId("cellA").build(),
                    RingSlot.newBuilder().setShard(1).setCellId("cellB").build()));

            // s2 no longer has a slot -> cellFor wraps by modulo (2 % 2 == 0 -> cellA), NOT cellC. No error thrown.
            List<String> where = api.doResolve(ResolveRequest.newBuilder().setInstanceId(s2).build())
                    .getEndpoint().getAddressesList();
            assertTrue(where.containsAll(List.of("grpc://a1:1", "grpc://a2:1")) && where.size() == 2,
                    "s2 wrongly wraps to cellA");
            assertFalse(where.contains("grpc://c1:1"),
                    "silent mis-route: s2's data is on cellC but it no longer resolves there");
        }
    }

    @Test @DisplayName("removing a shard via a NEW epoch keeps the existing instance resolvable while it drains (safe)")
    void removeShardViaNewEpochIsSafe() throws Exception {
        InMemoryCoordinatorStore store = new InMemoryCoordinatorStore();
        try (CoordinatorApi api = new CoordinatorApi(store, 0, Tls.Options.DISABLED)) {
            threeCellNamespace(api);                                  // epoch 0: 0->cellA, 1->cellB, 2->cellC
            String s2 = IdCodec.format("orders", 0, 2, Ids.token());

            // Reshard: new epoch omits shard 2. Old epoch 0 -> DRAINING (ring retained), epoch 1 OPEN.
            api.doOpenEpoch("orders", List.of(
                    RingSlot.newBuilder().setShard(0).setCellId("cellA").build(),
                    RingSlot.newBuilder().setShard(1).setCellId("cellB").build()));

            // The existing instance still resolves to cellC via the draining epoch -- no mis-route.
            assertEquals(List.of("grpc://c1:1"), api.doResolve(ResolveRequest.newBuilder().setInstanceId(s2).build())
                    .getEndpoint().getAddressesList(), "s2 still resolves to cellC (old epoch retains shard 2)");

            // A new instance in the current epoch routes within the reduced ring, never onto shard 2's cell.
            String newS0 = IdCodec.format("orders", 1, 0, Ids.token());
            List<String> newWhere = api.doResolve(ResolveRequest.newBuilder().setInstanceId(newS0).build())
                    .getEndpoint().getAddressesList();
            assertTrue(newWhere.containsAll(List.of("grpc://a1:1", "grpc://a2:1")) && newWhere.size() == 2,
                    "new epoch-1 instance routes to cellA");

            // cellC stays in the active set so its in-flight work keeps draining until the epoch retires.
            ActiveCellsResponse ac = api.doActiveCells("orders", null);
            assertTrue(ac.getCellsList().stream().anyMatch(e -> e.getTarget().equals("grpc://c1:1")),
                    "cellC is still polled while epoch 0 drains");
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
