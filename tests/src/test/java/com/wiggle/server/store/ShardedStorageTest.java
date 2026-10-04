package com.wiggle.server.store;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.flow.Wiggle;
import com.wiggle.core.WorkflowDefinition;
import com.wiggle.server.engine.DefinitionRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** What the sharded store itself guarantees: it routes, it refuses what it cannot route, and it owns its shards. */
class ShardedStorageTest {

    /** An in-memory store that records how many transactions reached it, and whether it was migrated
     *  and closed. */
    private static final class Probe implements Storage {
        final AtomicInteger txs = new AtomicInteger();
        final InMemoryStorage mem = new InMemoryStorage();
        boolean migrated, closed;
        final String fingerprint;

        Probe(String fingerprint) { this.fingerprint = fingerprint; }

        @Override public void migrate() { migrated = true; }
        @Override public <R> R inTx(Function<Tx, R> work) { txs.incrementAndGet(); return mem.inTx(work); }
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

    @Test @DisplayName("accounts go to the auth shard, which defaults to home and must be one of the shards")
    void authShard() {
        Probe seven = new Probe("a"), two = new Probe("b");
        ShardedStorage s = new ShardedStorage(List.of(new ShardedStorage.Member(7, ShardState.ACTIVE, false, seven),
                new ShardedStorage.Member(2, ShardState.ACTIVE, true, two)), 2, 7);
        s.inAuth(tx -> null);
        assertEquals(1, seven.txs.get());
        assertEquals(0, two.txs.get());
        assertEquals(7, s.auth());
        assertEquals(List.of(2), s.instanceShards(), "an auth-only shard holds no instances");
        assertEquals(2, new ShardedStorage(shards(new Probe("a"), new Probe("b")), 2).auth());
        assertThrows(IllegalArgumentException.class, () -> new ShardedStorage(
                List.of(new ShardedStorage.Member(2, ShardState.ACTIVE, true, new Probe("b"))), 2, 9));
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

    // -- binding to what the databases record --

    private static ShardedStorage.Member member(int id, ShardState state, Storage s) {
        return new ShardedStorage.Member(id, state, true, s);
    }

    @Test @DisplayName("each database is claimed for its shard, and a database claimed for another shard is refused")
    void identityIsClaimedAndChecked() {
        InMemoryStorage a = new InMemoryStorage(), b = new InMemoryStorage();
        new ShardedStorage(List.of(member(0, ShardState.ACTIVE, a), member(1, ShardState.ACTIVE, b)), 0).migrate();
        assertEquals(0, a.inTx(tx -> tx.shardIdentity()).orElseThrow());
        assertEquals(1, b.inTx(tx -> tx.shardIdentity()).orElseThrow());

        ShardedStorage swapped = new ShardedStorage(
                List.of(member(0, ShardState.ACTIVE, b), member(1, ShardState.ACTIVE, a)), 0);
        IllegalStateException e = assertThrows(IllegalStateException.class, swapped::migrate);
        assertTrue(e.getMessage().contains("points at the database of shard"), e.getMessage());
    }

    @Test @DisplayName("the registry on home remembers every shard, and a state only moves forward")
    void registryMovesForward() {
        InMemoryStorage home = new InMemoryStorage(), one = new InMemoryStorage();
        new ShardedStorage(List.of(member(0, ShardState.ACTIVE, home), member(1, ShardState.ACTIVE, one)), 0).migrate();
        assertEquals(List.of(0, 1), home.inTx(tx -> tx.shardRegistry()).stream().map(Rows.ShardRecord::shardId).toList());

        new ShardedStorage(List.of(member(0, ShardState.ACTIVE, home), member(1, ShardState.DRAINING, one)), 0).migrate();
        assertEquals(ShardState.DRAINING, home.inTx(tx -> tx.shardRegistry()).get(1).state());

        IllegalStateException back = assertThrows(IllegalStateException.class, () -> new ShardedStorage(
                List.of(member(0, ShardState.ACTIVE, home), member(1, ShardState.ACTIVE, one)), 0).migrate());
        assertTrue(back.getMessage().contains("only moves forward"), back.getMessage());
    }

    @Test @DisplayName("a shard the registry still holds live cannot be left out of the topology")
    void aLiveShardCannotBeOmitted() {
        InMemoryStorage home = new InMemoryStorage(), one = new InMemoryStorage();
        new ShardedStorage(List.of(member(0, ShardState.ACTIVE, home), member(1, ShardState.ACTIVE, one)), 0).migrate();
        IllegalStateException e = assertThrows(IllegalStateException.class, () ->
                new ShardedStorage(List.of(member(0, ShardState.ACTIVE, home)), 0).migrate());
        assertTrue(e.getMessage().contains("not listed"), e.getMessage());
    }

    @Test @DisplayName("a shard is retired only once empty; after that, an id naming it is not found")
    void retiring() {
        InMemoryStorage home = new InMemoryStorage(), one = new InMemoryStorage();
        new ShardedStorage(List.of(member(0, ShardState.ACTIVE, home), member(1, ShardState.ACTIVE, one)), 0).migrate();
        Rows.Instance live = new Rows.Instance();
        live.id = "wfi.s1.live";
        live.workflow = "w";
        live.version = 1;
        live.status = com.wiggle.core.InstanceStatus.RUNNING;
        live.context = com.wiggle.core.Doc.EMPTY;
        one.inTxVoid(tx -> tx.insertInstance(live));

        IllegalStateException busy = assertThrows(IllegalStateException.class, () -> new ShardedStorage(
                List.of(member(0, ShardState.ACTIVE, home), member(1, ShardState.RETIRED, one)), 0).migrate());
        assertTrue(busy.getMessage().contains("live instance"), busy.getMessage());

        one.inTxVoid(tx -> {
            Rows.Instance done = tx.findInstance(live.id).orElseThrow();
            done.status = com.wiggle.core.InstanceStatus.COMPLETED;
            tx.updateInstance(done);
        });
        ShardedStorage s = new ShardedStorage(
                List.of(member(0, ShardState.ACTIVE, home), member(1, ShardState.RETIRED, one)), 0);
        s.migrate();
        assertEquals(List.of(0), s.instanceShards(), "a retired shard is neither swept nor claimed from");
        assertThrows(ShardRetiredException.class, () -> s.inTxFor("wfi.s1.live", tx -> null));

        ShardedStorage dropped = new ShardedStorage(List.of(member(0, ShardState.ACTIVE, home)), 0);
        dropped.migrate();   // once retired, a shard may leave the topology
        assertThrows(ShardRetiredException.class, () -> dropped.inTxFor("wfi.s1.live", tx -> null),
                "and its ids are still not found, rather than unknown");
    }

    interface ForkSteps {
        Map<String, Object> a(Map<String, Object> ctx);
        Map<String, Object> b(Map<String, Object> ctx);
        Map<String, Object> pick(Map<String, Object> left, Map<String, Object> right);
    }

    @Test @DisplayName("a shard added to the topology receives every registered definition when it is migrated")
    void addedShardReceivesDefinitions() {
        WorkflowDefinition v1 = FlowSpec.define("grow", 1, Map.class, ForkSteps.class, (f, s) ->
                Wiggle.allOf(f.thenApply(s::a), f.thenApply(s::b)).combine(s::pick)).definition();
        WorkflowDefinition v2 = FlowSpec.define("grow", 2, Map.class, ForkSteps.class, (f, s) ->
                Wiggle.allOf(f.thenApply(s::b), f.thenApply(s::a)).combine(s::pick)).definition();
        InMemoryStorage home = new InMemoryStorage();
        InMemoryStorage added = new InMemoryStorage();

        ShardedStorage before = new ShardedStorage(Map.of(0, home), 0);
        before.migrate();
        DefinitionRegistry registry = new DefinitionRegistry(before);
        registry.register(v1);
        registry.register(v2);

        Map<Integer, Storage> grown = new LinkedHashMap<>();
        grown.put(0, home);
        grown.put(1, added);
        ShardedStorage after = new ShardedStorage(grown, 0);
        after.migrate();

        for (WorkflowDefinition def : List.of(v1, v2)) {
            added.inTx(tx -> {
                assertTrue(tx.definition(def.name(), def.version()).isPresent(), def.key() + " copied");
                assertEquals(def.fingerprint(), tx.definitionFingerprint(def.name(), def.version()).orElseThrow().value(),
                        def.key() + " keeps its fingerprint, so a re-registration is a no-op, not a conflict");
                assertEquals(def.numberOfNodes(), tx.graphNodeCount(def.name(), def.version()), def.key() + " graph rows");
                return null;
            });
        }
        assertEquals(List.of(1, 2), added.inTx(tx -> tx.definitionVersions("grow")));

        after.migrate();
        new DefinitionRegistry(after).register(v1);
        assertFalse(added.inTx(tx -> tx.definitionVersions("grow")).isEmpty(), "a second migrate and a re-registration change nothing");
    }
}
