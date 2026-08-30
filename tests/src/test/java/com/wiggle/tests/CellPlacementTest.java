package com.wiggle.tests;

import com.wiggle.core.Ids;
import com.wiggle.server.CellPlacement;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

    @Test @DisplayName("stampFor never splits an id's epoch from its shard across a concurrent set()")
    void stampForIsAtomicUnderReconfig() throws Exception {
        // Two generations with DISJOINT shard sets, so any (epoch,shard) mix is unambiguously a torn read.
        CellPlacement p = new CellPlacement(10, new int[]{10, 11});
        java.util.concurrent.atomic.AtomicBoolean stop = new java.util.concurrent.atomic.AtomicBoolean(false);
        java.util.concurrent.atomic.AtomicReference<String> tear = new java.util.concurrent.atomic.AtomicReference<>();

        Thread flipper = new Thread(() -> {
            boolean a = true;
            while (!stop.get()) {
                if (a) p.set(10, new int[]{10, 11}); else p.set(20, new int[]{20, 21});
                a = !a;
            }
        });
        Thread minter = new Thread(() -> {
            for (int i = 0; i < 2_000_000 && tear.get() == null; i++) {
                CellPlacement.Stamp s = p.stampFor(Ids.token());
                Set<Integer> expected = s.epoch() == 10 ? Set.of(10, 11) : Set.of(20, 21);
                if (!expected.contains(s.shard())) {
                    tear.set("epoch " + s.epoch() + " stamped with shard " + s.shard());
                }
            }
        });

        flipper.start(); minter.start();
        minter.join();
        stop.set(true);
        flipper.join();
        assertNull(tear.get(), "a mint saw a torn (epoch, shard) pair: " + tear.get());
    }

    @Test @DisplayName("set() re-points a running node; an empty shard set is standby, not genesis")
    void repoint() {
        CellPlacement p = new CellPlacement(0, new int[]{0});
        p.set(5, List.of(9));
        assertEquals(5, p.epoch());
        assertTrue(p.mintable());
        assertEquals(9, p.shardFor(Ids.token()));

        p.set(6, new int[]{});   // empty -> standby (a ring exists but does not name this cell)
        assertEquals(6, p.epoch());
        assertFalse(p.mintable(), "empty shard set means standby");
        assertThrows(IllegalStateException.class, () -> p.shardFor(Ids.token()), "standby cell refuses to mint");
        assertThrows(IllegalStateException.class, () -> p.stampFor(Ids.token()));
    }
}
