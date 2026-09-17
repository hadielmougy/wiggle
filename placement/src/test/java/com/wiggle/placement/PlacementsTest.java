package com.wiggle.placement;

import com.wiggle.core.Ids;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The placement rules, attacked with generated ring histories. The seed is fixed so a failure is
 * reproducible; it is printed in the assertion messages.
 */
class PlacementsTest {

    private static final long SEED = 20260916L;


    @Test @DisplayName("an id always resolves to the cell that owned its shard in ITS epoch, whatever came after")
    void resolutionIsStableAcrossLaterEpochs() {
        Random rnd = new Random(SEED);
        for (int trial = 0; trial < 2000; trial++) {
            List<String> cells = cells(rnd, 1 + rnd.nextInt(5));
            // build a history epoch by epoch, remembering the answer each one gave at the time
            Map<Long, Ring.Epoch> epochs = new LinkedHashMap<>();
            List<long[]> asked = new ArrayList<>();      // {epoch, shard}
            List<String> answered = new ArrayList<>();

            int history = 1 + rnd.nextInt(4);
            for (long e = 0; e < history; e++) {
                epochs.put(e, new Ring.Epoch(ring(rnd, cells), Ring.Status.OPEN));
                Ring.Policy soFar = new Ring.Policy("ns", e, epochs);
                int shard = rnd.nextInt(16);
                Placements.cellFor(soFar, e, shard).ifPresent(c -> { });
                asked.add(new long[]{e, shard});
                answered.add(Placements.cellFor(soFar, e, shard).orElse(null));
            }

            // now the whole history exists; every earlier answer must be unchanged
            Ring.Policy full = new Ring.Policy("ns", history - 1, epochs);
            for (int i = 0; i < asked.size(); i++) {
                long[] q = asked.get(i);
                assertEquals(answered.get(i), Placements.cellFor(full, q[0], (int) q[1]).orElse(null),
                        "trial " + trial + " (seed " + SEED + "): epoch " + q[0] + " shard " + q[1]
                        + " changed its answer once later epochs were added");
            }
        }
    }

    @Test @DisplayName("resolve is total: every shard of a non-empty ring lands on a cell in that ring")
    void everyShardLandsSomewhere() {
        Random rnd = new Random(SEED);
        for (int trial = 0; trial < 2000; trial++) {
            List<String> cells = cells(rnd, 1 + rnd.nextInt(4));
            List<Ring.Slot> slots = ring(rnd, cells);
            Ring.Policy policy = new Ring.Policy("ns", 0, Map.of(0L, new Ring.Epoch(slots, Ring.Status.OPEN)));
            Set<String> inRing = new java.util.HashSet<>();
            for (Ring.Slot s : slots) inRing.add(s.cellId());

            for (int shard = 0; shard < 32; shard++) {          // deliberately beyond the ring's size
                Optional<String> cell = Placements.cellFor(policy, 0, shard);
                assertTrue(cell.isPresent(),
                        "trial " + trial + " (seed " + SEED + "): shard " + shard + " resolved nowhere");
                assertTrue(inRing.contains(cell.get()),
                        "shard " + shard + " landed on '" + cell.get() + "', which is not in the ring");
            }
        }
    }

    @Test @DisplayName("a cell mints only the shards its ring gives it -- never another cell's")
    void aCellNeverMintsAnotherCellsShard() {
        Random rnd = new Random(SEED);
        for (int trial = 0; trial < 2000; trial++) {
            List<String> cells = cells(rnd, 2 + rnd.nextInt(4));
            List<Ring.Slot> slots = ring(rnd, cells);
            Ring.Policy policy = new Ring.Policy("ns", 0, Map.of(0L, new Ring.Epoch(slots, Ring.Status.OPEN)));

            for (String cell : cells) {
                Placements.Mintable m = Placements.mintable(policy, cell);
                for (int shard : m.shards()) {
                    assertEquals(cell, Placements.cellFor(policy, 0, shard).orElseThrow(),
                            "trial " + trial + " (seed " + SEED + "): '" + cell
                            + "' would mint shard " + shard + ", which belongs elsewhere");
                }
            }
        }
    }


    @Test @DisplayName("a ring that does not name a cell puts it on standby, distinct from no ring at all")
    void standbyIsNotTheSameAsUnplaced() {
        Ring.Policy placed = new Ring.Policy("ns", 0,
                Map.of(0L, new Ring.Epoch(List.of(new Ring.Slot(0, "cell-a", null)), Ring.Status.OPEN)));

        Placements.Mintable onStandby = Placements.mintable(placed, "cell-b");
        assertFalse(onStandby.allowed());
        assertSame(Placements.Reason.STANDBY, onStandby.reason(),
                "a ring exists and excludes this cell -- someone decided that");

        Placements.Mintable unplaced = Placements.mintable(null, "cell-b");
        assertFalse(unplaced.allowed());
        assertSame(Placements.Reason.NO_RING, unplaced.reason(),
                "nobody has placed this namespace -- nobody decided anything");
    }

    @Test @DisplayName("a draining epoch serves but does not mint")
    void drainingDoesNotMint() {
        Ring.Policy draining = new Ring.Policy("ns", 0,
                Map.of(0L, new Ring.Epoch(List.of(new Ring.Slot(0, "cell-a", null)), Ring.Status.DRAINING)));

        assertSame(Placements.Reason.NOT_OPEN, Placements.mintable(draining, "cell-a").reason());
        assertEquals("cell-a", Placements.cellFor(draining, 0, 0).orElseThrow(),
                "it still resolves -- the instances on it have to remain reachable");
    }

    @Test @DisplayName("the label wins over the ring, and only when that cell is live")
    void labelBeatsTheRingWhenLive() {
        Ring.Policy ringSaysA = new Ring.Policy("ns", 0,
                Map.of(0L, new Ring.Epoch(List.of(new Ring.Slot(0, "cell-a", null)), Ring.Status.OPEN)));

        String labelled = IdCodec.format("ns", "cell-b", 0, 0, Ids.token());
        IdCodec.Placement id = IdCodec.parse(labelled).orElseThrow();

        assertEquals("cell-b", Placements.resolve(id, ringSaysA, c -> true).orElseThrow(),
                "the instance was written on cell-b; the ring only says where the shard belongs");
        assertEquals("cell-a", Placements.resolve(id, ringSaysA, c -> false).orElseThrow(),
                "cell-b has no live nodes, so fall back to the ring rather than fail");

        String unlabelled = IdCodec.format("ns", 0, 0, Ids.token());
        assertEquals("cell-a", Placements.resolve(IdCodec.parse(unlabelled).orElseThrow(),
                        ringSaysA, c -> true).orElseThrow(),
                "no label, no change: the ring answers as it always did");
    }

    @Test @DisplayName("active cells are every cell still open or draining, and never a retired one")
    void activeCellsSpanLiveEpochs() {
        Map<Long, Ring.Epoch> epochs = new LinkedHashMap<>();
        epochs.put(0L, new Ring.Epoch(List.of(new Ring.Slot(0, "old-cell", null)), Ring.Status.RETIRED));
        epochs.put(1L, new Ring.Epoch(List.of(new Ring.Slot(0, "draining-cell", null)), Ring.Status.DRAINING));
        epochs.put(2L, new Ring.Epoch(List.of(new Ring.Slot(0, "new-cell", null)), Ring.Status.OPEN));

        List<String> active = Placements.activeCells(new Ring.Policy("ns", 2, epochs));
        assertTrue(active.contains("draining-cell"), "a draining cell still holds work a worker must poll");
        assertTrue(active.contains("new-cell"));
        assertFalse(active.contains("old-cell"), "retired means drained: nothing left to poll");
    }

    @Test @DisplayName("a cell only ever mints a shard it owns -- never index arithmetic into the shard space")
    void mintShardStaysWithinTheOwnedSet() {
        Random rnd = new Random(SEED);
        for (int trial = 0; trial < 3000; trial++) {
            // an arbitrary, non-contiguous set of owned shards -- the case that breaks a naive
            // implementation that hashes over the shard space instead of over the owned set
            List<Integer> owned = new ArrayList<>();
            int candidates = 1 + rnd.nextInt(12);
            for (int i = 0; i < candidates; i++) {
                int shard = rnd.nextInt(64);
                if (!owned.contains(shard)) owned.add(shard);
            }
            String ulid = Ids.token();

            int got = Placements.mintShard(owned, ulid);
            assertTrue(owned.contains(got), "trial " + trial + " (seed " + SEED + "): owned " + owned
                    + " minted " + got + ", which it does not hold");
            assertEquals(got, Placements.mintShard(owned, ulid),
                    "the same ulid must always land on the same shard");
        }
    }

    @Test @DisplayName("a cell owning nothing refuses to mint rather than falling back to shard 0")
    void standbyRefusesToMint() {
        IllegalStateException e = org.junit.jupiter.api.Assertions.assertThrows(
                IllegalStateException.class, () -> Placements.mintShard(List.of(), Ids.token()));
        assertTrue(e.getMessage().contains("standby"), e.getMessage());
    }


    private static List<String> cells(Random rnd, int n) {
        List<String> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) out.add("cell-" + (char) ('a' + i));
        return out;
    }

    /** A ring over a random subset of shards, each assigned to a random cell. */
    private static List<Ring.Slot> ring(Random rnd, List<String> cells) {
        int size = 1 + rnd.nextInt(8);
        List<Ring.Slot> slots = new ArrayList<>(size);
        for (int shard = 0; shard < size; shard++) {
            slots.add(new Ring.Slot(shard, cells.get(rnd.nextInt(cells.size())), null));
        }
        return slots;
    }
}
