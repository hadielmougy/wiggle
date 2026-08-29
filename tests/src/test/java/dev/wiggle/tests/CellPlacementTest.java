package dev.wiggle.tests;

import dev.wiggle.core.Ids;
import dev.wiggle.server.CellPlacement;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** T12 increment 2: the mutable placement a coordinator-managed cell mints into. */
class CellPlacementTest {

    @Test @DisplayName("defaults to epoch 0, shard 0 (the single implicit cell)")
    void defaults() {
        CellPlacement p = new CellPlacement();
        assertEquals(0, p.epoch());
        assertEquals(0, p.shardFor(Ids.token()));
    }

    @Test @DisplayName("stamps only shards the cell owns, spread across them")
    void stampsOwnedShards() {
        CellPlacement p = new CellPlacement(2, new int[]{3, 8});
        assertEquals(2, p.epoch());
        Set<Integer> seen = new HashSet<>();
        for (int i = 0; i < 200; i++) seen.add(p.shardFor(Ids.token()));
        assertTrue(Set.of(3, 8).containsAll(seen), "only owned shards are stamped, saw " + seen);
        assertEquals(Set.of(3, 8), seen, "both owned shards get used over many ids");
    }

    @Test @DisplayName("set() re-points a running node; an empty shard set falls back to shard 0")
    void repoint() {
        CellPlacement p = new CellPlacement(0, new int[]{0});
        p.set(5, List.of(9));
        assertEquals(5, p.epoch());
        assertEquals(9, p.shardFor(Ids.token()));

        p.set(6, new int[]{});   // empty -> genesis shard
        assertEquals(6, p.epoch());
        assertEquals(0, p.shardFor(Ids.token()));
    }
}
