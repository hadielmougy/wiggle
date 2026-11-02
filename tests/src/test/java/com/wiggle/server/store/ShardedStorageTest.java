package com.wiggle.server.store;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.flow.Wiggle;
import com.wiggle.core.WorkflowDefinition;
import com.wiggle.jdbc.JdbcStorage;
import com.wiggle.postgres.PostgresStorageFactory;
import com.wiggle.server.engine.DefinitionRegistry;
import com.wiggle.server.engine.InstanceIds;
import com.wiggle.server.engine.WorkflowEngine;
import com.wiggle.tests.TestTx;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** What the sharded store itself guarantees: it routes, it refuses what it cannot route, and it owns its shards. */
class ShardedStorageTest {

    /** An H2 store that records how many transactions reached it, and whether it was migrated
     *  and closed. */
    private static class Probe implements Storage {
        final AtomicInteger txs = new AtomicInteger();
        final JdbcStorage mem = PostgresStorageFactory.inMemory();
        boolean migrated, closed;
        final String fingerprint;

        Probe(String fingerprint) { this.fingerprint = fingerprint; }

        @Override public void migrate() { migrated = true; }
        @Override public <R> R inShard(int shard, Function<WriteRepositories, R> work) { txs.incrementAndGet(); return mem.inShard(shard, work); }
        @Override public String fingerprint() { return fingerprint; }
        @Override public void close() { closed = true; mem.close(); }
    }

    /** A {@link Probe} whose database can be taken out of reach. */
    private static final class Outageable extends Probe {
        volatile boolean unreachable;

        Outageable(String fingerprint) { super(fingerprint); }

        @Override public <R> R inShard(int shard, Function<WriteRepositories, R> work) {
            if (unreachable) {
                txs.incrementAndGet();
                throw new StorageUnreachableException("connection refused", null);
            }
            return super.inShard(shard, work);
        }
    }

    private static void await(BooleanSupplier condition, String what) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) throw new AssertionError("timed out waiting until " + what);
            Thread.sleep(10);
        }
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
        new ShardTransactions(s).forInstance("wfi.s7.01k6abc", () -> null);
        new ShardTransactions(s).readFor("tok.s7.01k6abc", Freshness.REPLICA_OK, () -> null);
        new ShardTransactions(s).inHome( () -> null);
        new ShardTransactions(s).forInstance("wfi_01k6abc", () -> null);   // carries no shard: home
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
        new ShardTransactions(s).inAuth( () -> null);
        assertEquals(1, seven.txs.get());
        assertEquals(0, two.txs.get());
        assertEquals(7, s.auth());
        assertEquals(List.of(2), s.instanceShards(), "an auth-only shard holds no instances");
        assertEquals(2, new ShardedStorage(shards(new Probe("a"), new Probe("b")), 2).auth());
        assertThrows(IllegalArgumentException.class, () -> new ShardedStorage(
                List.of(new ShardedStorage.Member(2, ShardState.ACTIVE, true, new Probe("b"))), 2, 9));
    }

    @Test @DisplayName("a shard this node does not know is a transient failure, never 'not found'")
    void unknownShardIsTransient() {
        ShardedStorage s = new ShardedStorage(shards(new Probe("a"), new Probe("b")), 2);
        StorageException e = assertThrows(StorageException.class, () -> new ShardTransactions(s).forInstance("wfi.s9.01k6abc", () -> null));
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
        JdbcStorage a = PostgresStorageFactory.inMemory(), b = PostgresStorageFactory.inMemory();
        new ShardedStorage(List.of(member(0, ShardState.ACTIVE, a), member(1, ShardState.ACTIVE, b)), 0).migrate();
        assertEquals(0, TestTx.inHome(a, () -> Repositories.shards().shardIdentity()).orElseThrow());
        assertEquals(1, TestTx.inHome(b, () -> Repositories.shards().shardIdentity()).orElseThrow());

        ShardedStorage swapped = new ShardedStorage(
                List.of(member(0, ShardState.ACTIVE, b), member(1, ShardState.ACTIVE, a)), 0);
        IllegalStateException e = assertThrows(IllegalStateException.class, swapped::migrate);
        assertTrue(e.getMessage().contains("points at the database of shard"), e.getMessage());
    }

    @Test @DisplayName("the registry on home remembers every shard, and a state only moves forward")
    void registryMovesForward() {
        JdbcStorage home = PostgresStorageFactory.inMemory(), one = PostgresStorageFactory.inMemory();
        new ShardedStorage(List.of(member(0, ShardState.ACTIVE, home), member(1, ShardState.ACTIVE, one)), 0).migrate();
        assertEquals(List.of(0, 1), TestTx.inHome(home, () -> Repositories.shards().shardRegistry()).stream().map(Rows.ShardRecord::shardId).toList());

        new ShardedStorage(List.of(member(0, ShardState.ACTIVE, home), member(1, ShardState.DRAINING, one)), 0).migrate();
        assertEquals(ShardState.DRAINING, TestTx.inHome(home, () -> Repositories.shards().shardRegistry()).get(1).state());

        IllegalStateException back = assertThrows(IllegalStateException.class, () -> new ShardedStorage(
                List.of(member(0, ShardState.ACTIVE, home), member(1, ShardState.ACTIVE, one)), 0).migrate());
        assertTrue(back.getMessage().contains("only moves forward"), back.getMessage());
    }

    @Test @DisplayName("a shard the registry still holds live cannot be left out of the topology")
    void aLiveShardCannotBeOmitted() {
        JdbcStorage home = PostgresStorageFactory.inMemory(), one = PostgresStorageFactory.inMemory();
        new ShardedStorage(List.of(member(0, ShardState.ACTIVE, home), member(1, ShardState.ACTIVE, one)), 0).migrate();
        IllegalStateException e = assertThrows(IllegalStateException.class, () ->
                new ShardedStorage(List.of(member(0, ShardState.ACTIVE, home)), 0).migrate());
        assertTrue(e.getMessage().contains("not listed"), e.getMessage());
    }

    @Test @DisplayName("a shard is retired only once empty; after that, an id naming it is not found")
    void retiring() {
        JdbcStorage home = PostgresStorageFactory.inMemory(), one = PostgresStorageFactory.inMemory();
        new ShardedStorage(List.of(member(0, ShardState.ACTIVE, home), member(1, ShardState.ACTIVE, one)), 0).migrate();
        Rows.Instance live = new Rows.Instance();
        live.id = "wfi.s1.live";
        live.workflow = "w";
        live.version = 1;
        live.status = com.wiggle.core.InstanceStatus.RUNNING;
        live.context = com.wiggle.core.Doc.EMPTY;
        TestTx.inHome(one, () -> Repositories.instances().insertInstance(live));

        IllegalStateException busy = assertThrows(IllegalStateException.class, () -> new ShardedStorage(
                List.of(member(0, ShardState.ACTIVE, home), member(1, ShardState.RETIRED, one)), 0).migrate());
        assertTrue(busy.getMessage().contains("live instance"), busy.getMessage());

        TestTx.inHome(one, () -> {
            Rows.Instance done = Repositories.instances().findInstance(live.id).orElseThrow();
            done.status = com.wiggle.core.InstanceStatus.COMPLETED;
            Repositories.instances().updateInstance(done);
        });
        ShardedStorage s = new ShardedStorage(
                List.of(member(0, ShardState.ACTIVE, home), member(1, ShardState.RETIRED, one)), 0);
        s.migrate();
        assertEquals(List.of(0), s.instanceShards(), "a retired shard is neither swept nor claimed from");
        assertThrows(ShardRetiredException.class, () -> new ShardTransactions(s).forInstance("wfi.s1.live", () -> null));

        ShardedStorage dropped = new ShardedStorage(List.of(member(0, ShardState.ACTIVE, home)), 0);
        dropped.migrate();   // once retired, a shard may leave the topology
        assertThrows(ShardRetiredException.class, () -> new ShardTransactions(dropped).forInstance("wfi.s1.live", () -> null),
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
        JdbcStorage home = PostgresStorageFactory.inMemory();
        JdbcStorage added = PostgresStorageFactory.inMemory();

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
            TestTx.inHome(added, () -> {
                assertTrue(Repositories.graph().definition(def.name(), def.version()).isPresent(), def.key() + " copied");
                assertEquals(def.fingerprint(), Repositories.graph().definitionFingerprint(def.name(), def.version()).orElseThrow().value(),
                        def.key() + " keeps its fingerprint, so a re-registration is a no-op, not a conflict");
                assertEquals(def.numberOfNodes(), Repositories.graph().graphNodeCount(def.name(), def.version()), def.key() + " graph rows");
                return null;
            });
        }
        assertEquals(List.of(1, 2), TestTx.inHome(added, () -> Repositories.graph().definitionVersions("grow")));

        after.migrate();
        new DefinitionRegistry(after).register(v1);
        assertFalse(TestTx.inHome(added, () -> Repositories.graph().definitionVersions("grow")).isEmpty(), "a second migrate and a re-registration change nothing");
    }

    @Test @DisplayName("a shard found unreachable fails its routes at once until a probe reaches it again")
    void unreachableShardFailsFast() throws InterruptedException {
        Outageable seven = new Outageable("a");
        Probe two = new Probe("b");
        ShardedStorage s = new ShardedStorage(shards(seven, two), 2);
        seven.unreachable = true;

        assertThrows(StorageUnreachableException.class, () -> new ShardTransactions(s).inShard(7, () -> null));
        assertTrue(s.isDown(7));
        assertEquals(1, seven.txs.get());

        StorageException refused = assertThrows(StorageUnreachableException.class,
                () -> new ShardTransactions(s).readFor("wfi.s7.01k6abc", Freshness.PRIMARY, () -> null));
        assertTrue(refused.repeatable(), "refused as transient, so a client sees UNAVAILABLE");
        await(() -> seven.txs.get() == 2, "the background probe has tried shard 7");
        await(() -> !isProbing(s), "the probe has backed off");
        assertThrows(StorageUnreachableException.class, () -> new ShardTransactions(s).inShard(7, () -> null));
        assertEquals(2, seven.txs.get(), "within the backoff, neither a route nor a probe reaches the shard");

        new ShardTransactions(s).inHome( () -> null);
        assertEquals(1, two.txs.get(), "the other shard is routed to as before");

        seven.unreachable = false;
        Thread.sleep(ShardedStorage.MIN_PROBE_BACKOFF_MILLIS);
        assertThrows(StorageUnreachableException.class, () -> new ShardTransactions(s).inShard(7, () -> null));
        await(() -> !s.isDown(7), "a probe finds shard 7 reachable");
        new ShardTransactions(s).inShard(7, () -> null);
    }

    private static boolean isProbing(ShardedStorage s) {
        return Thread.getAllStackTraces().keySet().stream().anyMatch(t -> t.getName().startsWith("wiggle-shard-probe-"));
    }

    @Test @DisplayName("a poll claims from the shards it reaches, and fails only when it reaches none")
    void claimPassesOverAnUnreachableShard() {
        WorkflowDefinition def = FlowSpec.define("reach", 1, Map.class, ForkSteps.class, (f, s) ->
                Wiggle.allOf(f.thenApply(s::a), f.thenApply(s::b)).combine(s::pick)).definition();
        Outageable seven = new Outageable("a");
        Outageable two = new Outageable("b");
        ShardedStorage s = new ShardedStorage(shards(seven, two), 2);
        s.migrate();
        WorkflowEngine engine = new WorkflowEngine(s, new DefinitionRegistry(s), 30_000, InstanceIds.onShard(2));
        new DefinitionRegistry(s).register(def);
        engine.start("reach", 1, Map.of(), null);
        seven.unreachable = true;

        int claimed = 0;
        for (int i = 0; i < 4; i++) claimed += engine.poll("w1", def.queues(), 1, null).size();
        assertEquals(2, claimed, "both tasks on shard 2 are claimed, whichever shard each poll tries first");

        two.unreachable = true;
        assertThrows(StorageUnreachableException.class, () -> engine.poll("w1", def.queues(), 1, null));
    }
}
