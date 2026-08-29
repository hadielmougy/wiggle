package dev.wiggle.server.coord;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Phase 1 / T6: coordinator reconciliation is leader-gated. A non-leader tick is a no-op; only the
 * leader expires dead nodes. (Same package as the reconciler so its package-visible {@code tick()}
 * can be driven deterministically, mirroring HousekeeperTest.)
 */
class CoordinatorReconcilerTest {

    @Test @DisplayName("only the leader expires dead nodes")
    void leaderGatedExpiry() {
        InMemoryCoordinatorStore store = new InMemoryCoordinatorStore();
        long now = System.currentTimeMillis();
        store.upsertNode(new CoordNode("stale", "ns", "grpc://h:1", "eu", "v", 1, 0));          // ancient heartbeat
        store.upsertNode(new CoordNode("fresh", "ns", "grpc://h:2", "eu", "v", 1, now));         // just now

        AtomicBoolean leader = new AtomicBoolean(false);
        CoordinatorReconciler r = new CoordinatorReconciler(store, leader::get, 60_000, 30_000);
        try {
            r.tick();   // not leader -> no-op
            assertEquals(2, store.nodes("ns").size(), "non-leader must not expire anything");

            leader.set(true);
            r.tick();   // leader -> the stale node (heartbeat 0) is older than now-30s
            assertEquals(1, store.nodes("ns").size());
            assertEquals("fresh", store.nodes("ns").get(0).id());
        } finally {
            r.close();
        }
    }
}
