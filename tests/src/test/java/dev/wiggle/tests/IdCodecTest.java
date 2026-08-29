package dev.wiggle.tests;

import dev.wiggle.core.IdCodec;
import dev.wiggle.core.Ids;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
}
