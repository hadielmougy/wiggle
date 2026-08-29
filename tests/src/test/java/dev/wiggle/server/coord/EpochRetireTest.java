package dev.wiggle.server.coord;

import dev.wiggle.server.coord.CoordPolicy.EpochRing;
import dev.wiggle.server.coord.CoordPolicy.EpochStatus;
import dev.wiggle.server.coord.CoordPolicy.RingSlot;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * T12 increment 3 (R21): the reconciler retires a DRAINING epoch once the census confirms it holds no
 * live instances on any cell, bumping the policy generation so nodes stop polling it. A silent or
 * non-empty epoch is left DRAINING. Same package as the reconciler to drive {@code retireDrained}/tick.
 */
class EpochRetireTest {

    /** orders: epoch 0 DRAINING (shard 0 -> cellA), epoch 1 OPEN (current). */
    private static long seedDrainingPolicy(InMemoryCoordinatorStore store) {
        Map<Long, EpochRing> epochs = new LinkedHashMap<>();
        epochs.put(0L, new EpochRing(List.of(new RingSlot(0, "cellA", null)), EpochStatus.DRAINING));
        epochs.put(1L, new EpochRing(List.of(new RingSlot(0, "cellA", null)), EpochStatus.OPEN));
        return store.casPolicy("orders", 0, new CoordPolicy("orders", 1, 0, epochs));
    }

    private static CoordinatorReconciler reconciler(InMemoryCoordinatorStore store, LiveCensus census, boolean leader) {
        return new CoordinatorReconciler(store, census, new AtomicBoolean(leader)::get, 60_000, 30_000);
    }

    @Test @DisplayName("a drained epoch with a fresh zero census is retired and the generation bumps")
    void retiresDrainedEpoch() {
        InMemoryCoordinatorStore store = new InMemoryCoordinatorStore();
        long rev = seedDrainingPolicy(store);
        LiveCensus census = new LiveCensus();
        census.record("a1", "orders", "cellA", Map.of(0L, 0L, 1L, 4L), System.currentTimeMillis()); // epoch0 empty

        reconciler(store, census, true).retireDrained("orders");

        CoordPolicy p = store.getPolicy("orders").orElseThrow();
        assertEquals(EpochStatus.RETIRED, p.epochs().get(0L).status(), "drained epoch 0 retired");
        assertEquals(EpochStatus.OPEN, p.epochs().get(1L).status(), "current epoch untouched");
        assertEquals(rev + 1, p.revision(), "retire bumps the policy generation");
    }

    @Test @DisplayName("a draining epoch that still holds work is not retired")
    void keepsNonEmptyEpoch() {
        InMemoryCoordinatorStore store = new InMemoryCoordinatorStore();
        long rev = seedDrainingPolicy(store);
        LiveCensus census = new LiveCensus();
        census.record("a1", "orders", "cellA", Map.of(0L, 3L), System.currentTimeMillis()); // epoch0 still busy

        reconciler(store, census, true).retireDrained("orders");

        CoordPolicy p = store.getPolicy("orders").orElseThrow();
        assertEquals(EpochStatus.DRAINING, p.epochs().get(0L).status(), "still draining");
        assertEquals(rev, p.revision(), "no change, no generation bump");
    }

    @Test @DisplayName("without a fresh census the epoch is left draining (safe direction)")
    void noCensusNoRetire() {
        InMemoryCoordinatorStore store = new InMemoryCoordinatorStore();
        long rev = seedDrainingPolicy(store);

        reconciler(store, new LiveCensus(), true).retireDrained("orders");

        CoordPolicy p = store.getPolicy("orders").orElseThrow();
        assertEquals(EpochStatus.DRAINING, p.epochs().get(0L).status());
        assertEquals(rev, p.revision());
    }

    @Test @DisplayName("retire is leader-gated: a non-leader tick changes nothing")
    void leaderGated() {
        InMemoryCoordinatorStore store = new InMemoryCoordinatorStore();
        long rev = seedDrainingPolicy(store);
        LiveCensus census = new LiveCensus();
        census.record("a1", "orders", "cellA", Map.of(0L, 0L), System.currentTimeMillis());

        CoordinatorReconciler r = reconciler(store, census, false);
        try {
            r.tick();   // not leader
            assertEquals(EpochStatus.DRAINING, store.getPolicy("orders").orElseThrow().epochs().get(0L).status());
            assertEquals(rev, store.getPolicy("orders").orElseThrow().revision());
        } finally {
            r.close();
        }
    }
}
