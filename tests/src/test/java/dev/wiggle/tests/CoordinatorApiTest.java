package dev.wiggle.tests;

import dev.wiggle.core.Tls;
import dev.wiggle.proto.EpochStatus;
import dev.wiggle.proto.Policy;
import dev.wiggle.proto.RingSlot;
import dev.wiggle.server.coord.CoordinatorApi;
import dev.wiggle.server.coord.InMemoryCoordinatorStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Phase 1 / T6: the coordinator's admin policy writes. OpenEpoch creates then appends epochs and
 * SetRing updates one -- all CAS-guarded read-modify-writes over the store. Exercised via the public
 * logic methods (no gRPC plumbing needed).
 */
class CoordinatorApiTest {

    private static RingSlot slot(int shard, String cell) {
        return RingSlot.newBuilder().setShard(shard).setCellId(cell).setRegion("eu-west").build();
    }

    @Test @DisplayName("openEpoch creates epoch 0, then appends epoch 1 and drains the previous")
    void openEpochCreatesThenAppends() throws Exception {
        try (CoordinatorApi api = new CoordinatorApi(new InMemoryCoordinatorStore(), 0, Tls.Options.DISABLED)) {
            Policy p0 = api.doOpenEpoch("acme", List.of(slot(0, "cell-3")));
            assertEquals(0, p0.getCurrentEpoch());
            assertEquals(1, p0.getRevision());
            assertEquals(EpochStatus.OPEN, p0.getEpochsOrThrow(0).getStatus());
            assertEquals("cell-3", p0.getEpochsOrThrow(0).getRing(0).getCellId());

            Policy p1 = api.doOpenEpoch("acme", List.of(slot(0, "cell-5")));
            assertEquals(1, p1.getCurrentEpoch());
            assertEquals(2, p1.getRevision());
            assertEquals(EpochStatus.DRAINING, p1.getEpochsOrThrow(0).getStatus(), "previous epoch drains");
            assertEquals(EpochStatus.OPEN, p1.getEpochsOrThrow(1).getStatus());
            assertEquals("cell-5", p1.getEpochsOrThrow(1).getRing(0).getCellId());
        }
    }

    @Test @DisplayName("setRing replaces an epoch's ring and bumps the revision")
    void setRingUpdatesEpoch() throws Exception {
        try (CoordinatorApi api = new CoordinatorApi(new InMemoryCoordinatorStore(), 0, Tls.Options.DISABLED)) {
            api.doOpenEpoch("acme", List.of(slot(0, "cell-3")));
            Policy p = api.doSetRing("acme", 0, List.of(slot(0, "cell-9")));
            assertEquals("cell-9", p.getEpochsOrThrow(0).getRing(0).getCellId());
            assertEquals(2, p.getRevision());
            assertEquals(EpochStatus.OPEN, p.getEpochsOrThrow(0).getStatus(), "status preserved");
        }
    }
}
