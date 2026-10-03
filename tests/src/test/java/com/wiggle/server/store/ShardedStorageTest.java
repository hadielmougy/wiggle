package com.wiggle.server.store;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** What the sharded store itself guarantees: it routes, it refuses what it cannot route, and it owns its shards. */
class ShardedStorageTest {

    /** Records which shard a transaction reached, and whether it was migrated and closed. */
    private static final class Probe implements Storage {
        final AtomicInteger txs = new AtomicInteger();
        boolean migrated, closed;
        final String fingerprint;

        Probe(String fingerprint) { this.fingerprint = fingerprint; }

        @Override public void migrate() { migrated = true; }
        @Override public <R> R inTx(Function<Tx, R> work) { txs.incrementAndGet(); return null; }
        @Override public String fingerprint() { return fingerprint; }
        @Override public void close() { closed = true; }
    }

    private static Map<Integer, Storage> shards(Storage seven, Storage two) {
        Map<Integer, Storage> m = new LinkedHashMap<>();
        m.put(7, seven);
        m.put(2, two);
        return m;
    }

    @Test @DisplayName("each entry point reaches the one shard it names")
    void routes() {
        Probe seven = new Probe("a"), two = new Probe("b");
        ShardedStorage s = new ShardedStorage(shards(seven, two), 2);
        s.inTxFor("wfi.s7.01k6abc", tx -> null);
        s.readFor("tok.s7.01k6abc", Freshness.REPLICA_OK, tx -> null);
        s.inHome(tx -> null);
        s.inTxFor("wfi_01k6abc", tx -> null);   // carries no shard: home
        assertEquals(2, seven.txs.get());
        assertEquals(2, two.txs.get());
        assertEquals(List.of(7, 2), s.instanceShards(), "fan-out visits shards in configured order");
        assertEquals(2, s.home());
    }

    @Test @DisplayName("an unrouted transaction is refused")
    void unroutedIsRefused() {
        ShardedStorage s = new ShardedStorage(shards(new Probe("a"), new Probe("b")), 2);
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> s.inTx(tx -> null));
        assertTrue(e.getMessage().contains("unrouted"), e.getMessage());
    }

    @Test @DisplayName("a shard this node does not know is a transient failure, never 'not found'")
    void unknownShardIsTransient() {
        ShardedStorage s = new ShardedStorage(shards(new Probe("a"), new Probe("b")), 2);
        StorageException e = assertThrows(StorageException.class, () -> s.inTxFor("wfi.s9.01k6abc", tx -> null));
        assertSame(StorageException.Classification.TRANSIENT, e.classification());
    }

    @Test @DisplayName("the home shard must be one of the shards")
    void homeMustBeAShard() {
        assertThrows(IllegalArgumentException.class, () -> new ShardedStorage(shards(new Probe("a"), new Probe("b")), 3));
        assertThrows(IllegalArgumentException.class, () -> new ShardedStorage(Map.of(), 0));
    }

    @Test @DisplayName("migrate and close reach every shard")
    void lifecycleReachesEveryShard() {
        Probe seven = new Probe("a"), two = new Probe("b");
        ShardedStorage s = new ShardedStorage(shards(seven, two), 2);
        s.migrate();
        s.close();
        assertTrue(seven.migrated && two.migrated);
        assertTrue(seven.closed && two.closed);
    }

    @Test @DisplayName("the fingerprint names every shard, and is absent when any shard has none")
    void fingerprint() {
        assertEquals("7=a,2=b", new ShardedStorage(shards(new Probe("a"), new Probe("b")), 2).fingerprint());
        assertNull(new ShardedStorage(shards(new Probe("a"), new Probe(null)), 2).fingerprint());
    }
}
