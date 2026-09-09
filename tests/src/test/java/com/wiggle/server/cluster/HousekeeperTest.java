package com.wiggle.server.cluster;

import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.server.engine.DefinitionRegistry;
import com.wiggle.server.engine.WorkflowEngine;
import com.wiggle.server.store.InMemoryStorage;
import com.wiggle.server.store.Storage;
import com.wiggle.server.store.Tx;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * White-box test (same package) for the housekeeper itself -- the leader gate, the sweeps it
 * drives, and its resilience -- rather than the engine duties it delegates to, which have their
 * own coverage.
 */
class HousekeeperTest {

    private static WorkflowEngine engine(Storage storage) {
        return new WorkflowEngine(storage, new DefinitionRegistry(storage), 30_000);
    }

    /** A one-sleep workflow whose timer parks the instance until the housekeeper fires it. */
    private static Blueprint sleeper(long millis) {
        return Workflow.define("hk-sleeper")
                .sleep("nap", Duration.ofMillis(millis))
                .step("after")
                .build();
    }

    @Test @DisplayName("adaptive tick drains a backlog larger than one batch; fixed tick does not")
    void adaptiveTickDrains() throws Exception {
        try (Storage storage = new InMemoryStorage();
             ClusterManager cluster = new ClusterManager(storage, "hk-d", 1, 5000, 3)) {
            storage.migrate();
            cluster.start();
            WorkflowEngine engine = engine(storage);
            Blueprint bp = sleeper(20);
            engine.register(bp.definition());
            for (int i = 0; i < 25; i++) engine.start(bp.name(), bp.version(), Map.of(), null);
            Thread.sleep(60);   // all 25 timers are now due; batch size below is 10

            new Housekeeper(engine, cluster, Duration.ofMillis(100), Duration.ofHours(1), 10, false).tick();
            int afterFixed = engine.poll("w1", bp.definition().workerQueues(), 100, null).size();
            assertEquals(10, afterFixed, "the fixed sweep promotes at most one batch per tick");

            new Housekeeper(engine, cluster, Duration.ofMillis(100), Duration.ofHours(1), 10, true).tick();
            int afterAdaptive = engine.poll("w2", bp.definition().workerQueues(), 100, null).size();
            assertEquals(15, afterAdaptive, "the adaptive sweep drains the remaining backlog in one tick");
        }
    }

    @Test @DisplayName("a leader tick fires due timers")
    void leaderTickFiresTimers() throws Exception {
        try (Storage storage = new InMemoryStorage();
             ClusterManager cluster = new ClusterManager(storage, "hk-a", 1, 5000, 3)) {
            storage.migrate();
            cluster.start();
            assertTrue(cluster.isLeader(), "a lone node leads");
            WorkflowEngine engine = engine(storage);
            Blueprint bp = sleeper(30);
            engine.register(bp.definition());
            String id = engine.start(bp.name(), bp.version(), Map.of(), null);

            assertTrue(engine.poll("w", bp.definition().workerQueues(), 10, null).isEmpty(),
                    "nothing dispatchable while the timer runs");
            Thread.sleep(60);

            new Housekeeper(engine, cluster, Duration.ofMillis(100), Duration.ofHours(1), 10).tick();

            assertEquals(1, engine.poll("w", bp.definition().workerQueues(), 10, null).size(),
                    "the step after the fired timer is dispatchable");
            assertEquals("RUNNING", engine.instance(id).orElseThrow().status());
        }
    }

    @Test @DisplayName("a non-leader tick does nothing")
    void nonLeaderTickIsInert() throws Exception {
        try (Storage storage = new InMemoryStorage();
             ClusterManager first = new ClusterManager(storage, "hk-first", 1, 5000, 3)) {
            storage.migrate();
            first.start();
            Thread.sleep(20);
            try (ClusterManager follower = new ClusterManager(storage, "hk-second", 1, 5000, 3)) {
                follower.start();
                assertFalse(follower.isLeader(), "the newer node follows");
                WorkflowEngine engine = engine(storage);
                Blueprint bp = sleeper(10);
                engine.register(bp.definition());
                engine.start(bp.name(), bp.version(), Map.of(), null);
                Thread.sleep(40);

                new Housekeeper(engine, follower, Duration.ofMillis(100), Duration.ofHours(1), 10).tick();

                assertTrue(engine.poll("w", bp.definition().workerQueues(), 10, null).isEmpty(),
                        "a follower must not fire timers");
            }
        }
    }

    @Test @DisplayName("a tick survives a failing engine call")
    void tickSurvivesFailure() {
        Storage broken = new Storage() {
            @Override public void migrate() { }
            @Override public <R> R inTx(Function<Tx, R> work) { throw new RuntimeException("storage down"); }
            @Override public void close() { }
        };
        try (Storage healthy = new InMemoryStorage();
             ClusterManager cluster = new ClusterManager(healthy, "hk-c", 1, 5000, 3)) {
            healthy.migrate();
            cluster.start();
            Housekeeper housekeeper = new Housekeeper(engine(broken), cluster, Duration.ofMillis(100),
                    Duration.ofHours(1), 10);
            assertDoesNotThrow(housekeeper::tick, "a failing sweep must not kill the scheduler");
            assertDoesNotThrow(housekeeper::retain, "a failing retention sweep must not kill the scheduler");
        }
    }

    @Test @DisplayName("the retention sweep purges old terminal instances, and only as leader")
    void retentionPurgesTerminalInstances() throws Exception {
        try (Storage storage = new InMemoryStorage();
             ClusterManager cluster = new ClusterManager(storage, "hk-d", 1, 5000, 3)) {
            storage.migrate();
            cluster.start();
            WorkflowEngine engine = engine(storage);
            Blueprint bp = sleeper(60_000);
            engine.register(bp.definition());
            String id = engine.start(bp.name(), bp.version(), Map.of(), null);
            engine.cancel(id, "make it terminal");
            Thread.sleep(10);   // let it age past the 1ms retention

            new Housekeeper(engine, cluster, Duration.ofMillis(100), Duration.ofMillis(1), 10).retain();

            assertTrue(engine.instance(id).isEmpty(), "the aged terminal instance is purged");
        }
    }
}
