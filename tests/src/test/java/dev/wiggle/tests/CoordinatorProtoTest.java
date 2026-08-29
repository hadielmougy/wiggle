package dev.wiggle.tests;

import com.google.protobuf.ByteString;
import dev.wiggle.proto.CellCoordinatorGrpc;
import dev.wiggle.proto.EpochRing;
import dev.wiggle.proto.EpochStatus;
import dev.wiggle.proto.NodeConfig;
import dev.wiggle.proto.Policy;
import dev.wiggle.proto.RegisterWorkflowRequest;
import dev.wiggle.proto.ResolveRequest;
import dev.wiggle.proto.RingSlot;
import dev.wiggle.proto.StorageSpec;
import dev.wiggle.proto.Tuning;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase 1 / T4: the {@code coordinator.proto} compiles and its generated types behave -- maps,
 * {@code oneof}, {@code optional} presence, nested messages, and {@code bytes} all round-trip, and the
 * {@code CellCoordinator} gRPC service class is generated.
 */
class CoordinatorProtoTest {

    @Test @DisplayName("the CellCoordinator gRPC service class is generated with the right name")
    void serviceGenerated() {
        assertEquals("dev.wiggle.proto.CellCoordinator", CellCoordinatorGrpc.SERVICE_NAME);
    }

    @Test @DisplayName("NodeConfig round-trips (nested message + sparse optional tuning)")
    void nodeConfigRoundTrips() throws Exception {
        Tuning tuning = Tuning.newBuilder()
                .setLeaseMillis(30_000)                    // set
                .setMemory(Tuning.Memory.newBuilder().setSheddingEnabled(true).setThreshold(0.9).build())
                .build();                                  // pollIntervalMillis intentionally unset
        NodeConfig nc = NodeConfig.newBuilder()
                .setNamespace("acme")
                .setGeneration(7)
                .setStorage(StorageSpec.newBuilder()
                        .setScheme("postgres").setJdbcUrl("jdbc:postgresql://pg/acme")
                        .setUser("acme_app").setSecretRef("vault://db/acme#pw").setPoolSize(16).build())
                .setTuning(tuning)
                .setTtlSeconds(300)
                .build();

        NodeConfig back = NodeConfig.parseFrom(nc.toByteArray());
        assertEquals(nc, back);
        assertEquals(7, back.getGeneration());
        assertTrue(back.getTuning().hasLeaseMillis(), "lease was set");
        assertFalse(back.getTuning().hasPollIntervalMillis(), "poll interval left to the node default");
        assertEquals("postgres", back.getStorage().getScheme());
    }

    @Test @DisplayName("Policy round-trips its epoch->ring map")
    void policyMapRoundTrips() throws Exception {
        Policy p = Policy.newBuilder()
                .setNamespace("acme")
                .setCurrentEpoch(7)
                .setRevision(3)
                .putEpochs(6L, EpochRing.newBuilder()
                        .addRing(RingSlot.newBuilder().setShard(0).setCellId("cell-3").setRegion("eu-west").build())
                        .setStatus(EpochStatus.DRAINING).build())
                .putEpochs(7L, EpochRing.newBuilder()
                        .addRing(RingSlot.newBuilder().setShard(0).setCellId("cell-5").setRegion("eu-west").build())
                        .setStatus(EpochStatus.OPEN).build())
                .build();

        Policy back = Policy.parseFrom(p.toByteArray());
        assertEquals(p, back);
        assertEquals(EpochStatus.OPEN, back.getEpochsOrThrow(7L).getStatus());
        assertEquals("cell-3", back.getEpochsOrThrow(6L).getRing(0).getCellId());
    }

    @Test @DisplayName("ResolveRequest oneof and RegisterWorkflow bytes round-trip")
    void oneofAndBytes() throws Exception {
        ResolveRequest byNs = ResolveRequest.newBuilder().setNamespace("acme").setCallerRegion("eu-west").build();
        assertEquals(ResolveRequest.ByCase.NAMESPACE, byNs.getByCase());
        ResolveRequest byId = ResolveRequest.newBuilder().setInstanceId("acme.e7.s2.ULID").build();
        assertEquals(ResolveRequest.ByCase.INSTANCE_ID, ResolveRequest.parseFrom(byId.toByteArray()).getByCase());

        RegisterWorkflowRequest rw = RegisterWorkflowRequest.newBuilder()
                .setNamespace("acme").setName("order")
                .setDefinition(ByteString.copyFromUtf8("{\"name\":\"order\"}"))
                .build();
        assertEquals(rw, RegisterWorkflowRequest.parseFrom(rw.toByteArray()));
    }
}
