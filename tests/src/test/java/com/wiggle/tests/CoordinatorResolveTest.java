package com.wiggle.tests;

import com.wiggle.core.IdCodec;
import com.wiggle.core.Ids;
import com.wiggle.core.Tls;
import com.wiggle.proto.ActiveCellsResponse;
import com.wiggle.proto.Endpoint;
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
 * Phase 2 / T9: Resolve turns a namespace or instance id into a cell {@link Endpoint} from the live
 * roster (region-filtered); ActiveCells reports the cell set plus a change generation.
 */
class CoordinatorResolveTest {

    private static RegisteredNode node(String endpoint, String region) {
        return RegisteredNode.newBuilder().setName(endpoint).setEndpoint(endpoint).setRegion(region).build();
    }

    private static RegisteredNode node(String endpoint, String region, String cellId) {
        return RegisteredNode.newBuilder().setName(endpoint).setEndpoint(endpoint)
                .setRegion(region).setCellId(cellId).build();
    }

    private static CoordinatorApi api(InMemoryCoordinatorStore store) throws Exception {
        return new CoordinatorApi(store, 0, Tls.Options.DISABLED);
    }

    @Test @DisplayName("resolve by namespace returns the live roster as a cell endpoint")
    void resolveByNamespace() throws Exception {
        InMemoryCoordinatorStore store = new InMemoryCoordinatorStore();
        try (CoordinatorApi api = api(store)) {
            api.doRegister("nsA", node("grpc://ha:1", "eu-west", "cell-1"));
            api.doRegister("nsA", node("grpc://hb:2", "eu-west", "cell-1"));
            api.doOpenEpoch("nsA", List.of(RingSlot.newBuilder().setShard(0).setCellId("cell-1").build()));

            ResolveResponse r = api.doResolve(ResolveRequest.newBuilder().setNamespace("nsA").build());
            assertEquals("nsA", r.getNamespace());
            assertEquals(0, r.getEpoch());
            assertFalse(r.getEndpoint().getTarget().isBlank());
            assertTrue(r.getEndpoint().getAddressesList().containsAll(List.of("grpc://ha:1", "grpc://hb:2")));
            assertEquals(30, r.getTtlSeconds());
        }
    }

    @Test @DisplayName("resolve by instance id extracts the namespace and routes to its cell")
    void resolveByInstanceId() throws Exception {
        InMemoryCoordinatorStore store = new InMemoryCoordinatorStore();
        try (CoordinatorApi api = api(store)) {
            api.doRegister("nsA", node("grpc://ha:1", "eu-west"));
            String id = IdCodec.format("nsA", 0, 0, Ids.token());
            ResolveResponse r = api.doResolve(ResolveRequest.newBuilder().setInstanceId(id).build());
            assertEquals("nsA", r.getNamespace());
            assertEquals(List.of("grpc://ha:1"), r.getEndpoint().getAddressesList());
        }
    }

    @Test @DisplayName("resolving a legacy instance id is rejected")
    void resolveLegacyRejected() throws Exception {
        InMemoryCoordinatorStore store = new InMemoryCoordinatorStore();
        try (CoordinatorApi api = api(store)) {
            api.doRegister("nsA", node("grpc://ha:1", "eu-west"));
            assertThrows(IllegalArgumentException.class,
                    () -> api.doResolve(ResolveRequest.newBuilder().setInstanceId("wfi_01h8abcdef").build()));
        }
    }

    @Test @DisplayName("resolving a namespace with no live nodes fails")
    void resolveNoNodes() throws Exception {
        InMemoryCoordinatorStore store = new InMemoryCoordinatorStore();
        try (CoordinatorApi api = api(store)) {
            assertThrows(IllegalStateException.class,
                    () -> api.doResolve(ResolveRequest.newBuilder().setNamespace("empty").build()));
        }
    }

    @Test @DisplayName("resolution returns region-appropriate addresses (same cell, R24)")
    void regionFiltering() throws Exception {
        InMemoryCoordinatorStore store = new InMemoryCoordinatorStore();
        try (CoordinatorApi api = api(store)) {
            api.doRegister("nsA", node("grpc://eu1:1", "eu-west"));
            api.doRegister("nsA", node("grpc://eu2:1", "eu-west"));
            api.doRegister("nsA", node("grpc://us1:1", "us-east"));

            ResolveResponse us = api.doResolve(
                    ResolveRequest.newBuilder().setNamespace("nsA").setCallerRegion("us-east").build());
            assertEquals(List.of("grpc://us1:1"), us.getEndpoint().getAddressesList(), "only the us-east node");

            ResolveResponse all = api.doResolve(ResolveRequest.newBuilder().setNamespace("nsA").build());
            assertEquals(3, all.getEndpoint().getAddressesList().size(), "no region filter -> all nodes");
        }
    }

    @Test @DisplayName("activeCells reports the cell set and a generation tied to the policy revision")
    void activeCells() throws Exception {
        InMemoryCoordinatorStore store = new InMemoryCoordinatorStore();
        try (CoordinatorApi api = api(store)) {
            api.doRegister("nsA", node("grpc://ha:1", "eu-west", "cell-1"));
            api.doOpenEpoch("nsA", List.of(RingSlot.newBuilder().setShard(0).setCellId("cell-1").build())); // revision 1

            ActiveCellsResponse ac = api.doActiveCells("nsA", null);
            assertEquals(1, ac.getGeneration(), "generation follows the policy revision");
            assertEquals(1, ac.getCellsList().size());
            assertEquals(List.of("grpc://ha:1"), ac.getCells(0).getAddressesList());
        }
    }
}
