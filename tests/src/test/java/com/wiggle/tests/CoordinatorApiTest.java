package com.wiggle.tests;

import com.wiggle.core.Tls;
import com.wiggle.proto.EpochStatus;
import com.wiggle.proto.Policy;
import com.wiggle.proto.RingSlot;
import com.wiggle.server.coord.CoordinatorApi;
import com.wiggle.server.coord.CoordinatorService;
import com.wiggle.server.coord.InMemoryCoordinatorStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
            Policy p0 = api.service().doOpenEpoch("acme", List.of(slot(0, "cell-3")));
            assertEquals(0, p0.getCurrentEpoch());
            assertEquals(1, p0.getRevision());
            assertEquals(EpochStatus.OPEN, p0.getEpochsOrThrow(0).getStatus());
            assertEquals("cell-3", p0.getEpochsOrThrow(0).getRing(0).getCellId());

            Policy p1 = api.service().doOpenEpoch("acme", List.of(slot(0, "cell-5")));
            assertEquals(1, p1.getCurrentEpoch());
            assertEquals(2, p1.getRevision());
            assertEquals(EpochStatus.DRAINING, p1.getEpochsOrThrow(0).getStatus(), "previous epoch drains");
            assertEquals(EpochStatus.OPEN, p1.getEpochsOrThrow(1).getStatus());
            assertEquals("cell-5", p1.getEpochsOrThrow(1).getRing(0).getCellId());
        }
    }

    @Test @DisplayName("reaper dead-timeout is derived from the node heartbeat interval, always exceeding it")
    void reaperTimeoutTracksNodeHeartbeat() {
        long beatMillis = CoordinatorService.NODE_HEARTBEAT_INTERVAL_SECONDS * 1000L;
        // Whatever the missed-count (even a misconfigured 0/1), the dead timeout must exceed one beat,
        // so a live node that heartbeats on schedule is never reaped.
        for (int missed : new int[]{0, 1, 2, 3, 5}) {
            assertTrue(CoordinatorService.nodeDeadMillis(missed) > beatMillis,
                    "dead timeout must exceed the node heartbeat interval for missed=" + missed);
        }
        assertEquals(3 * beatMillis, CoordinatorService.nodeDeadMillis(3));
        assertEquals(2 * beatMillis, CoordinatorService.nodeDeadMillis(1), "floored at two beats");
    }

    @Test @DisplayName("setRing rejects a slot change -- a published epoch's ring is sealed")
    void setRingRejectsSlotChange() throws Exception {
        try (CoordinatorApi api = new CoordinatorApi(new InMemoryCoordinatorStore(), 0, Tls.Options.DISABLED)) {
            api.service().doOpenEpoch("acme", List.of(slot(0, "cell-3")));
            assertThrows(IllegalArgumentException.class,
                    () -> api.service().doSetRing("acme", 0, List.of(slot(0, "cell-9"))),
                    "reshaping a sealed epoch's ring in place is forbidden");
            // The ring is untouched by the rejected write.
            Policy after = api.service().doOpenEpoch("acme", List.of(slot(0, "cell-3")));  // read via a fresh append
            assertEquals("cell-3", after.getEpochsOrThrow(0).getRing(0).getCellId(), "epoch 0 ring unchanged");
        }
    }

    @Test @DisplayName("setRing with the epoch's exact ring is an idempotent no-op (no revision bump)")
    void setRingNoOpIsIdempotent() throws Exception {
        try (CoordinatorApi api = new CoordinatorApi(new InMemoryCoordinatorStore(), 0, Tls.Options.DISABLED)) {
            Policy opened = api.service().doOpenEpoch("acme", List.of(slot(0, "cell-3")));
            Policy p = api.service().doSetRing("acme", 0, List.of(slot(0, "cell-3")));
            assertEquals(opened.getRevision(), p.getRevision(), "a no-op setRing does not bump the revision");
            assertEquals("cell-3", p.getEpochsOrThrow(0).getRing(0).getCellId());
            assertEquals(EpochStatus.OPEN, p.getEpochsOrThrow(0).getStatus(), "status preserved");
        }
    }
}
