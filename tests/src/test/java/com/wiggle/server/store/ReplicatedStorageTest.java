package com.wiggle.server.store;

import com.wiggle.server.store.ReplicatedStorage.Fallback;
import com.wiggle.server.store.ReplicatedStorage.Named;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Which store a read reaches. Each store here is a separate in-memory database holding one marker
 * instance, so a read names the store that served it.
 */
class ReplicatedStorageTest {

    private static final long NOW = 1_000_000L;

    /** A store that can be broken, to stand in for a replica that stops answering. */
    private static final class Breakable implements Storage {
        final InMemoryStorage mem = new InMemoryStorage();
        volatile boolean broken;

        @Override public void migrate() { }
        @Override public <R> R inTx(Function<Tx, R> work) {
            if (broken) throw new StorageException("replica down", null, StorageException.Classification.TRANSIENT);
            return mem.inTx(work);
        }
        @Override public void close() { }
    }

    private static <S extends Storage> S marked(S s, String marker, Long beatAt) {
        s.inTx(tx -> {
            Rows.Instance i = new Rows.Instance();
            i.id = "marker";
            i.workflow = marker;
            i.version = 1;
            i.status = com.wiggle.core.InstanceStatus.RUNNING;
            i.context = com.wiggle.core.Doc.EMPTY;
            tx.insertInstance(i);
            tx.claimShardIdentity(0);
            if (beatAt != null) tx.writeShardBeat(beatAt);
            return null;
        });
        return s;
    }

    private static String servedBy(Storage s, Freshness f) {
        return s.readShard(0, f, tx -> tx.findInstance("marker").map(i -> i.workflow).orElseThrow());
    }

    private static ReplicatedStorage store(Fallback fallback, Storage primary, Storage... replicas) {
        List<Named> named = new ArrayList<>();
        for (int i = 0; i < replicas.length; i++) named.add(new Named("#" + (i + 1), replicas[i]));
        return new ReplicatedStorage(0, primary, named, 5_000, fallback, () -> NOW);
    }

    @Test @DisplayName("a replica-allowed read goes to a healthy replica; a primary read and every write go to the primary")
    void routes() {
        Storage primary = marked(new InMemoryStorage(), "primary", null);
        ReplicatedStorage s = store(Fallback.PRIMARY, primary, marked(new InMemoryStorage(), "replica", NOW - 100));
        s.probeReplicas(NOW);
        assertEquals("replica", servedBy(s, Freshness.REPLICA_OK));
        assertEquals("primary", servedBy(s, Freshness.PRIMARY));
        assertEquals("primary", s.inTx(tx -> tx.findInstance("marker").orElseThrow().workflow));
        assertEquals("primary", s.inShard(0, tx -> tx.findInstance("marker").orElseThrow().workflow));
        assertEquals(List.of(new ReplicatedStorage.ReplicaStatus("#1", true, 100, null)), s.replicaStatus());
    }

    @Test @DisplayName("until a replica has been probed, and while it lags too far, the primary serves")
    void unprobedOrLaggingFallsBack() {
        Storage primary = marked(new InMemoryStorage(), "primary", null);
        ReplicatedStorage s = store(Fallback.PRIMARY, primary, marked(new InMemoryStorage(), "replica", NOW - 9_000));
        assertEquals("primary", servedBy(s, Freshness.REPLICA_OK), "not probed yet");
        s.probeReplicas(NOW);
        assertEquals("primary", servedBy(s, Freshness.REPLICA_OK), "9 s behind, over the 5 s bound");
        assertFalse(s.replicaStatus().getFirst().healthy());
        assertEquals(9_000, s.replicaStatus().getFirst().lagMillis());
    }

    @Test @DisplayName("a replica with no heartbeat yet serves nothing")
    void noHeartbeatYet() {
        ReplicatedStorage s = store(Fallback.PRIMARY, marked(new InMemoryStorage(), "primary", null),
                marked(new InMemoryStorage(), "replica", null));
        s.probeReplicas(NOW);
        assertEquals("primary", servedBy(s, Freshness.REPLICA_OK));
        assertEquals(-1, s.replicaStatus().getFirst().lagMillis());
    }

    @Test @DisplayName("healthy replicas take reads in turn")
    void roundRobin() {
        ReplicatedStorage s = store(Fallback.PRIMARY, marked(new InMemoryStorage(), "primary", null),
                marked(new InMemoryStorage(), "a", NOW), marked(new InMemoryStorage(), "b", NOW));
        s.probeReplicas(NOW);
        List<String> served = new ArrayList<>();
        for (int i = 0; i < 4; i++) served.add(servedBy(s, Freshness.REPLICA_OK));
        assertEquals(List.of("a", "b", "a", "b"), served);
    }

    @Test @DisplayName("with fallback FAIL, a read no replica can serve is a transient failure")
    void failFallback() {
        ReplicatedStorage s = store(Fallback.FAIL, marked(new InMemoryStorage(), "primary", null),
                marked(new InMemoryStorage(), "replica", NOW - 60_000));
        s.probeReplicas(NOW);
        StorageException e = assertThrows(StorageException.class, () -> servedBy(s, Freshness.REPLICA_OK));
        assertSame(StorageException.Classification.TRANSIENT, e.classification());
        assertEquals("primary", servedBy(s, Freshness.PRIMARY), "primary reads are unaffected");
    }

    @Test @DisplayName("a replica that fails a read is left out, then probed again after a backoff")
    void ejectAndRecover() {
        Breakable replica = marked(new Breakable(), "replica", NOW);
        ReplicatedStorage s = store(Fallback.PRIMARY, marked(new InMemoryStorage(), "primary", null), replica);
        s.probeReplicas(NOW);
        replica.broken = true;
        assertEquals("primary", servedBy(s, Freshness.REPLICA_OK), "the failed read is served by the fallback");
        assertFalse(s.replicaStatus().getFirst().healthy());

        replica.broken = false;
        replica.mem.inTx(tx -> { tx.writeShardBeat(NOW + 500); return null; });
        s.probeReplicas(NOW + 500);
        assertFalse(s.replicaStatus().getFirst().healthy(), "inside the backoff it is not probed");
        s.probeReplicas(NOW + ReplicatedStorage.MIN_BACKOFF_MILLIS + 1);
        assertTrue(s.replicaStatus().getFirst().healthy(), "after it, a good probe restores it");
        assertEquals("replica", servedBy(s, Freshness.REPLICA_OK));
    }

    @Test @DisplayName("the heartbeat is written on the primary, once it is claimed for its shard")
    void heartbeat() {
        InMemoryStorage primary = new InMemoryStorage();
        ReplicatedStorage s = store(Fallback.PRIMARY, primary, new InMemoryStorage());
        s.migrate();
        s.beatPrimaries(NOW);
        assertEquals(Optional.of(NOW), primary.inTx(tx -> tx.shardBeat().isPresent()
                ? Optional.of(tx.shardBeat().getAsLong()) : Optional.empty()));
        assertTrue(s.hasReplicas());
    }
}
