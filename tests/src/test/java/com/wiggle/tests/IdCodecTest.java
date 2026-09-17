package com.wiggle.tests;

import com.wiggle.placement.IdCodec;
import com.wiggle.core.Ids;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase 2 / T8: the epoch-aware id format round-trips, tolerates legacy ids, and rejects a namespace
 * that would break parsing.
 */
class IdCodecTest {

    @Test @DisplayName("format then parse round-trips")
    void roundTrip() {
        String ulid = Ids.token();
        String id = IdCodec.format("acme", 7, 2, ulid);
        assertEquals("acme.e7.s2." + ulid, id);
        IdCodec.Placement p = IdCodec.parse(id).orElseThrow();
        assertEquals("acme", p.namespace());
        assertEquals(7, p.epoch());
        assertEquals(2, p.shard());
        assertEquals(ulid, p.ulid());
        assertFalse(IdCodec.isLegacy(id));
    }

    @Test @DisplayName("a ulid may contain dots; only the namespace segment is dot-free")
    void ulidWithDots() {
        String id = IdCodec.format("ns", 0, 0, "weird.ulid.value");
        IdCodec.Placement p = IdCodec.parse(id).orElseThrow();
        assertEquals("ns", p.namespace());
        assertEquals("weird.ulid.value", p.ulid());
    }

    @Test @DisplayName("legacy ids do not parse and are flagged legacy")
    void legacy() {
        assertTrue(IdCodec.parse("wfi_01h8abcdeffghijklmnopqrst").isEmpty());
        assertTrue(IdCodec.isLegacy(Ids.next("wfi")));
        assertTrue(IdCodec.isLegacy("nodotshere"));
        assertTrue(IdCodec.parse(null).isEmpty());
    }

    @Test @DisplayName("a namespace containing '.' is rejected at format")
    void badNamespace() {
        assertThrows(IllegalArgumentException.class, () -> IdCodec.format("a.b", 0, 0, "x"));
        assertThrows(IllegalArgumentException.class, () -> IdCodec.format("", 0, 0, "x"));
    }

    @Test @DisplayName("shardFor is 0 for a single-cell ring and in-range otherwise")
    void shardFor() {
        assertEquals(0, IdCodec.shardFor("anything", 1));
        long s = IdCodec.shardFor(Ids.token(), 4);
        assertTrue(s >= 0 && s < 4);
    }

    @Test @DisplayName("shardFor is deterministic for a given ulid")
    void shardForDeterministic() {
        String ulid = Ids.token();
        assertEquals(IdCodec.shardFor(ulid, 8), IdCodec.shardFor(ulid, 8));
    }

    @Test @DisplayName("shardFor spreads ULIDs roughly evenly across a ring")
    void shardForBalance() {
        int ring = 8, n = 100_000;
        int[] hits = new int[ring];
        for (int i = 0; i < n; i++) hits[(int) IdCodec.shardFor(Ids.token(), ring)]++;
        double expected = n / (double) ring;
        for (int h : hits) {
            assertTrue(Math.abs(h - expected) < expected * 0.1,
                    "each shard within 10% of even, got " + java.util.Arrays.toString(hits));
        }
    }


    @Test @DisplayName("an id carries the cell that minted it, and round-trips")
    void cellRoundTrip() {
        String ulid = Ids.token();
        String id = IdCodec.format("acme", "cell-a", 7, 2, ulid);
        assertEquals("acme.ccell-a.e7.s2." + ulid, id);

        IdCodec.Placement p = IdCodec.parse(id).orElseThrow();
        assertEquals("acme", p.namespace());
        assertEquals("cell-a", p.cellId());
        assertTrue(p.hasCell());
        assertEquals(7, p.epoch());
        assertEquals(2, p.shard());
        assertEquals(ulid, p.ulid());
    }

    @Test @DisplayName("an id minted before cells still parses, with no cell -- not an error")
    void preCellIdsStillParse() {
        String ulid = Ids.token();
        String old = IdCodec.format("acme", 7, 2, ulid);          // the four-arg form
        assertEquals("acme.e7.s2." + ulid, old, "the pre-cell format is unchanged");

        IdCodec.Placement p = IdCodec.parse(old).orElseThrow();
        assertNull(p.cellId());
        assertFalse(p.hasCell(), "no label means 'ask the placement policy', as before");
        assertEquals(7, p.epoch());
        assertEquals(ulid, p.ulid());
    }

    @Test @DisplayName("the label sits before the epoch so a dotted ulid cannot be read as a cell")
    void aDottedUlidIsNotMistakenForACell() {
        // The hazard the position guards against: with the label after the shard, this legacy id
        // would parse as cell 'foo' with ulid 'bar', silently routing an instance to a cell that
        // never held it. format() allows dots in a ulid, so this id is constructible.
        String id = IdCodec.format("ns", 0, 0, "cfoo.bar");
        assertEquals("ns.e0.s0.cfoo.bar", id);

        IdCodec.Placement p = IdCodec.parse(id).orElseThrow();
        assertNull(p.cellId(), "no cell segment here -- the ulid merely starts with 'c'");
        assertEquals("cfoo.bar", p.ulid());
    }

    @Test @DisplayName("a cell id with a dot is rejected, like a namespace")
    void cellMustBeOneSegment() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> IdCodec.format("ns", "cell.a", 0, 0, Ids.token()));
        assertTrue(e.getMessage().contains("cell id"), e.getMessage());
    }

    @Test @DisplayName("a blank cell id omits the segment rather than minting an empty one")
    void blankCellIsOmitted() {
        String ulid = Ids.token();
        assertEquals("ns.e0.s0." + ulid, IdCodec.format("ns", "  ", 0, 0, ulid));
        assertEquals("ns.e0.s0." + ulid, IdCodec.format("ns", null, 0, 0, ulid));
    }

    @Test @DisplayName("an id too long for its column fails at mint, naming the reason")
    void lengthIsGuardedAtMint() {
        // id columns are VARCHAR(128) from schema v9; the budget is namespace + cell + 31 for a
        // single-digit epoch and shard. Failing here beats failing at the insert with a column error.
        String longNs = "a".repeat(100);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> IdCodec.format(longNs, "some-rather-long-cell-name", 0, 0, Ids.token()));
        assertTrue(e.getMessage().contains("over the " + IdCodec.MAX_LENGTH), e.getMessage());

        // and a realistic pair fits with room to spare
        String ok = IdCodec.format("orders", "pooled-cell-3", 0, 0, Ids.token());
        assertTrue(ok.length() <= IdCodec.MAX_LENGTH, ok + " is " + ok.length());
    }
}
