package com.wiggle.postgres;

import com.wiggle.core.Doc;
import com.wiggle.core.InstanceStatus;
import com.wiggle.jdbc.JdbcStorage;
import com.wiggle.server.store.Freshness;
import com.wiggle.server.store.ReplicatedStorage;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.ShardedStorage;
import com.wiggle.server.topology.Topology;
import com.wiggle.server.topology.TopologyParser;
import com.wiggle.tests.TestDb;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Read replicas against a real PostgreSQL hot standby streaming from {@code WIGGLE_TEST_PG_URL}.
 * Opt-in: set {@code WIGGLE_TEST_PG_REPLICA_URL} to the standby; the user must be allowed to pause
 * WAL replay on it.
 */
@EnabledIfEnvironmentVariable(named = "WIGGLE_TEST_PG_REPLICA_URL", matches = ".+")
class PostgresReplicaTest {

    private static final String REPLICA = System.getenv("WIGGLE_TEST_PG_REPLICA_URL");

    private static void onStandby(String sql) throws SQLException {
        try (Connection c = DriverManager.getConnection(REPLICA, TestDb.user("PG"), TestDb.password("PG"));
             Statement s = c.createStatement()) {
            s.execute(sql);
        }
    }

    private static void await(BooleanSupplier condition, String what) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) throw new AssertionError("timed out waiting until " + what);
            Thread.sleep(100);
        }
    }

    private static Rows.Instance instance(String id) {
        Rows.Instance i = new Rows.Instance();
        i.id = id;
        i.workflow = "replica-test";
        i.version = 1;
        i.status = InstanceStatus.RUNNING;
        i.context = Doc.EMPTY;
        i.createdAt = i.updatedAt = System.currentTimeMillis();
        return i;
    }

    /** One monitor round, as the leader would run it: stamp, give it a moment to replicate, probe. */
    private static void tick(ShardedStorage storage) throws InterruptedException {
        storage.beatPrimaries(System.currentTimeMillis());
        Thread.sleep(150);
        storage.probeReplicas(System.currentTimeMillis());
    }

    @Test @DisplayName("a hot standby serves replica reads, refuses writes, and drops out when it falls behind")
    void hotStandby() throws Exception {
        Topology t = TopologyParser.fromEnvironment(Map.of(
                "WIGGLE_JDBC_URL", TestDb.url("PG"), "WIGGLE_JDBC_USER", TestDb.user("PG"),
                "WIGGLE_JDBC_PASSWORD", TestDb.password("PG"), "WIGGLE_JDBC_REPLICA_URLS", REPLICA,
                "WIGGLE_JDBC_MAX_REPLICA_LAG_MILLIS", "1000")).orElseThrow();
        try (ShardedStorage storage = PostgresStorageFactory.sharded(t)) {
            storage.migrate();
            ReplicatedStorage shard = (ReplicatedStorage) storage.members().getFirst().storage();

            tick(storage);
            await(() -> {
                try { tick(storage); } catch (InterruptedException e) { throw new RuntimeException(e); }
                return shard.replicaStatus().getFirst().healthy();
            }, "the standby is within its lag bound");

            // Pause replay: a row written now is on the primary and not yet on the standby, so which
            // database answered a read is visible in the answer.
            onStandby("SELECT pg_wal_replay_pause()");
            try {
                String id = "wfi.s0.replica-" + System.nanoTime();
                storage.inTxFor(id, tx -> { tx.insertInstance(instance(id)); return null; });
                assertTrue(storage.readFor(id, Freshness.PRIMARY, tx -> tx.findInstance(id)).isPresent());
                assertTrue(storage.readFor(id, Freshness.REPLICA_OK, tx -> tx.findInstance(id)).isEmpty(),
                        "the replica-allowed read went to the paused standby");

                Thread.sleep(1_200);
                tick(storage);
                assertFalse(shard.replicaStatus().getFirst().healthy(), shard.replicaStatus().toString());
                assertTrue(storage.readFor(id, Freshness.REPLICA_OK, tx -> tx.findInstance(id)).isPresent(),
                        "once the standby is over its lag bound, the primary serves");
            } finally {
                onStandby("SELECT pg_wal_replay_resume()");
            }
            await(() -> {
                try { tick(storage); } catch (InterruptedException e) { throw new RuntimeException(e); }
                return shard.replicaStatus().getFirst().healthy();
            }, "the standby has caught up and serves again");
        }

        try (JdbcStorage replica = JdbcStorage.readReplica(REPLICA, TestDb.user("PG"), TestDb.password("PG"), 1,
                new PostgresDialect())) {
            RuntimeException refused = assertThrows(RuntimeException.class, () -> replica.inTx(tx -> {
                tx.writeShardBeat(System.currentTimeMillis());
                return null;
            }), "a replica connection cannot write");
            assertTrue(String.valueOf(refused.getMessage()).contains("read-only"), refused.getMessage());
        }
    }
}
