package com.wiggle.server.engine;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.core.ExecutionMode;
import com.wiggle.core.InstanceView;
import com.wiggle.core.ShardIds;
import com.wiggle.core.TaskActivation;
import com.wiggle.server.engine.WorkflowEngine.Run;
import com.wiggle.server.engine.WorkflowEngine.RunResult;
import com.wiggle.server.engine.WorkflowEngine.StepInput;
import com.wiggle.server.store.InMemoryStorage;
import com.wiggle.server.store.ShardedStorage;
import com.wiggle.server.store.Storage;
import com.wiggle.tests.Modes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The engine over two shards: where instances land, and the paths that only branch when there is
 * more than one shard -- a claim moving between shards, a report batch spanning them, reads merged
 * across them, and leader sweeps visiting each.
 */
class ShardedEngineTest {

    interface TwoSteps {
        Map<String, Object> x(Map<String, Object> ctx);
        Map<String, Object> y(Map<String, Object> ctx);
    }

    private static final FlowSpec FLOW = FlowSpec.define("sharded", 1, Map.class, TwoSteps.class,
            (f, s) -> Modes.in(f, ExecutionMode.LOCAL_ASYNC).thenApply(s::x).thenApply(s::y));

    /** Shard 1 first, so the first instance lands off the home shard (0). */
    private record Fixture(InMemoryStorage one, InMemoryStorage zero, ShardedStorage storage, WorkflowEngine engine) {
        static Fixture open() {
            InMemoryStorage one = new InMemoryStorage();
            InMemoryStorage zero = new InMemoryStorage();
            Map<Integer, Storage> shards = new LinkedHashMap<>();
            shards.put(1, one);
            shards.put(0, zero);
            ShardedStorage storage = new ShardedStorage(shards, 0);
            storage.migrate();
            WorkflowEngine engine = new WorkflowEngine(storage, new DefinitionRegistry(storage), 30_000,
                    InstanceIds.across(storage.instanceShards()));
            engine.register(FLOW.definition());
            return new Fixture(one, zero, storage, engine);
        }

        Storage shard(String id) {
            return ShardIds.shardOf(id).getAsInt() == 1 ? one : zero;
        }

        List<String> startFour() {
            return List.of(start(), start(), start(), start());
        }

        String start() {
            return engine.start(FLOW.name(), FLOW.version(), Map.of(), null);
        }

        Set<String> queues() {
            return FLOW.definition().queues();
        }
    }

    private static Run fullRun(TaskActivation first) {
        String y = FLOW.definition().node(first.nodeId()).next();
        return new Run(first.taskId(), "w", List.of(
                new StepInput(first.nodeId(), Map.of("x", 1L), null),
                new StepInput(y, Map.of("x", 1L, "y", 2L), null)), true);
    }

    @Test @DisplayName("instances take the shards in turn and live only in the database their id names")
    void instancesLiveOnTheShardTheirIdNames() {
        Fixture f = Fixture.open();
        List<String> ids = f.startFour();
        assertEquals(List.of(1, 0, 1, 0), ids.stream().map(id -> ShardIds.shardOf(id).getAsInt()).toList());
        for (String id : ids) {
            Storage holder = f.shard(id);
            Storage other = holder == f.one() ? f.zero() : f.one();
            assertTrue(holder.inTx(tx -> tx.findInstance(id)).isPresent(), id + " is on its shard");
            assertTrue(other.inTx(tx -> tx.findInstance(id)).isEmpty(), id + " is nowhere else");
            holder.inTx(tx -> tx.tokensOf(id)).forEach(t ->
                    assertEquals(ShardIds.shardOf(id), ShardIds.shardOf(t.id), "its tokens are with it"));
        }
    }

    @Test @DisplayName("registration writes the definition to every shard")
    void registrationReachesEveryShard() {
        Fixture f = Fixture.open();
        assertTrue(f.one().inTx(tx -> tx.latestVersion(FLOW.name())).isPresent());
        assertTrue(f.zero().inTx(tx -> tx.latestVersion(FLOW.name())).isPresent());
    }

    @Test @DisplayName("a claim takes one shard at a time and the next claim reaches the other")
    void aClaimMovesBetweenShards() {
        Fixture f = Fixture.open();
        f.startFour();
        List<TaskActivation> first = f.engine().poll("w", f.queues(), 10, null);
        List<TaskActivation> second = f.engine().poll("w", f.queues(), 10, null);
        assertEquals(2, first.size(), "one shard's work per claim");
        assertEquals(2, second.size());
        Set<Integer> firstShards = new HashSet<>(), secondShards = new HashSet<>();
        first.forEach(t -> firstShards.add(ShardIds.shardOf(t.taskId()).getAsInt()));
        second.forEach(t -> secondShards.add(ShardIds.shardOf(t.taskId()).getAsInt()));
        assertEquals(1, firstShards.size());
        assertEquals(1, secondShards.size());
        assertTrue(!firstShards.equals(secondShards), "the second claim went to the other shard");
        assertTrue(f.engine().poll("w", f.queues(), 10, null).isEmpty(), "both shards drained");
    }

    @Test @DisplayName("a report batch spanning both shards advances every run")
    void aBatchAcrossShardsAdvancesEveryRun() {
        Fixture f = Fixture.open();
        List<String> ids = f.startFour();
        List<TaskActivation> claimed = new java.util.ArrayList<>(f.engine().poll("w", f.queues(), 10, null));
        claimed.addAll(f.engine().poll("w", f.queues(), 10, null));
        assertEquals(4, claimed.size());

        Map<String, RunResult> results = f.engine().report(claimed.stream().map(ShardedEngineTest::fullRun).toList());

        assertEquals(claimed.stream().map(TaskActivation::taskId).toList(), List.copyOf(results.keySet()),
                "one answer per run, in submission order");
        results.forEach((task, r) -> assertTrue(r.ok(), task + ": " + r));
        for (String id : ids) assertEquals("COMPLETED", f.engine().instance(id).orElseThrow().status());
    }

    @Test @DisplayName("lists, backlog and queue depth merge both shards")
    void readsMergeBothShards() {
        Fixture f = Fixture.open();
        List<String> ids = f.startFour();
        List<InstanceView> listed = f.engine().list(null, null, 100);
        assertEquals(Set.copyOf(ids), Set.copyOf(listed.stream().map(InstanceView::id).toList()));
        for (int i = 1; i < listed.size(); i++) {
            assertTrue(listed.get(i - 1).createdAt() >= listed.get(i).createdAt(), "newest first across shards");
        }
        assertEquals(2, f.engine().list(null, null, 2).size(), "the limit applies to the merged list");
        assertEquals(4, f.engine().queueDepth().readyCount());
        assertEquals(1, f.engine().backlog(10).size(), "one (workflow, version, queue) slice, summed");
        assertEquals(4, f.engine().backlog(10).getFirst().readyCount());
    }

    @Test @DisplayName("the leader's lease reclaim visits every shard")
    void sweepsVisitEveryShard() throws InterruptedException {
        Fixture f = Fixture.open();
        f.startFour();
        f.engine().poll("w", f.queues(), 10, 1L);
        f.engine().poll("w", f.queues(), 10, 1L);
        Thread.sleep(20);
        assertEquals(4, f.engine().reclaimExpiredLeases(100), "two orphans on each shard, all reclaimed");
    }

    @Test @DisplayName("a schedule fires its instance on the home shard, where the schedule lives")
    void aScheduleFiresOnTheHomeShard() throws InterruptedException {
        Fixture f = Fixture.open();
        f.engine().createSchedule(FLOW.name(), Duration.ofMillis(1), Map.of());
        Thread.sleep(20);
        assertEquals(1, f.engine().fireDueSchedules(10));
        List<InstanceView> fired = f.engine().list(FLOW.name(), null, 10);
        assertEquals(1, fired.size());
        assertEquals(OptionalInt.of(0), ShardIds.shardOf(fired.getFirst().id()));
        assertTrue(f.zero().inTx(tx -> tx.findInstance(fired.getFirst().id())).isPresent());
    }
}
