package dev.wiggle.tests;

import dev.wiggle.server.coord.LiveCensus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** T12 increment 3: the coordinator's rolling live-by-epoch view, aggregated for retire decisions. */
class LiveCensusTest {

    @Test @DisplayName("nodes of one cell share a DB (take max); cells sum (disjoint DBs)")
    void maxWithinCellSumAcrossCells() {
        LiveCensus c = new LiveCensus();
        long now = 1_000_000;
        // cellA: two nodes see the same DB -> both report epoch0=5 (a slight skew: one lags at 4)
        c.record("a1", "orders", "cellA", Map.of(0L, 5L), now);
        c.record("a2", "orders", "cellA", Map.of(0L, 4L), now);
        // cellB: its own DB -> epoch0=3
        c.record("b1", "orders", "cellB", Map.of(0L, 3L), now);

        LiveCensus.Aggregate agg = c.aggregate("orders", now);
        assertTrue(agg.hasFresh());
        assertEquals(8, agg.count(0L), "max(5,4) in cellA + 3 in cellB = 8, not 12");
    }

    @Test @DisplayName("stale reports are ignored; a silent namespace reports no fresh data")
    void staleIgnored() {
        LiveCensus c = new LiveCensus();
        c.record("a1", "orders", "cellA", Map.of(0L, 2L), 500);   // old

        assertFalse(c.aggregate("orders", 1_000).hasFresh(), "nothing fresh -> hasFresh=false");
        assertEquals(0, c.aggregate("orders", 1_000).count(0L));

        c.record("a1", "orders", "cellA", Map.of(0L, 0L), 1_500); // fresh, zero live
        LiveCensus.Aggregate agg = c.aggregate("orders", 1_000);
        assertTrue(agg.hasFresh());
        assertEquals(0, agg.count(0L));
    }

    @Test @DisplayName("forget drops a node's report")
    void forget() {
        LiveCensus c = new LiveCensus();
        c.record("a1", "orders", "cellA", Map.of(0L, 7L), 1_000);
        c.forget("a1");
        assertFalse(c.aggregate("orders", 1_000).hasFresh());
    }
}
