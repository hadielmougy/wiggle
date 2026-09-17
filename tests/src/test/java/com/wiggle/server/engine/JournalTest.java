package com.wiggle.server.engine;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.core.NodeKind;
import com.wiggle.core.TaskActivation;
import com.wiggle.jdbc.JdbcStorage;
import com.wiggle.postgres.H2Dialect;
import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.Rows.InstanceStatus;
import com.wiggle.server.store.Rows.Token;
import com.wiggle.server.store.Rows.TokenStatus;
import com.wiggle.server.store.Storage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The single-writer journal: acked means mirrored, un-acked means invisible, one writer per
 *  database, and a fenced-out zombie stalls instead of corrupting. */
@Timeout(60)
class JournalTest {

    private static Storage h2(String label) {
        JdbcStorage s = new JdbcStorage(
                "jdbc:h2:mem:" + label + "-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
                "sa", "", 4, new H2Dialect());
        s.migrate();
        return s;
    }

    interface Steps {
        Map<String, Object> a(Map<String, Object> c);
        Map<String, Object> b(Map<String, Object> c);
        Map<String, Object> c(Map<String, Object> c);
    }

    @Test @DisplayName("a whole flow runs journal-mode: memory-resident, mirrored, COMPLETED in the DB")
    void endToEnd() throws Exception {
        try (Storage storage = h2("journal-e2e")) {
            WorkflowEngine engine = new WorkflowEngine(storage, new DefinitionRegistry(storage, 256),
                    20_000, () -> "wfi_j" + System.nanoTime(), true);
            try {
                assertTrue(engine.journal.active(), "single node claims the lease");
                FlowSpec spec = FlowSpec.define("journal-e2e", Map.class, Steps.class,
                        (f, s) -> f.thenApply(s::a).thenApply(s::b).thenApply(s::c));
                engine.register(spec.definition());
                String id = engine.start(spec.name(), spec.version(), Map.of("v", 0), null);

                int completed = 0;
                long deadline = System.currentTimeMillis() + 30_000;
                while (completed < 3 && System.currentTimeMillis() < deadline) {
                    List<TaskActivation> tasks = engine.poll("jw", Set.of(), null, 4, 5_000L,
                            System.currentTimeMillis() + 1_000, () -> false);
                    for (TaskActivation t : tasks) {
                        engine.complete(t.taskId(), t.leaseOwner(), Map.of("v", ++completed));
                    }
                }
                assertEquals(3, completed, "all three steps dispatched and completed");

                Instance mirrored = storage.inTx(tx -> tx.findInstance(id)).orElseThrow();
                assertSame(InstanceStatus.COMPLETED, mirrored.status,
                        "the terminal state is in the database, because the last ack waited for it");
            } finally {
                engine.close();
            }
        }
    }

    @Test @DisplayName("an ack means the mirror has it: complete() returns only after the flush")
    void ackMeansFlushed() throws Exception {
        try (Storage storage = h2("journal-ack")) {
            WorkflowEngine engine = new WorkflowEngine(storage, new DefinitionRegistry(storage, 256),
                    20_000, () -> "wfi_a" + System.nanoTime(), true);
            try {
                FlowSpec spec = FlowSpec.define("journal-ack", Map.class, Steps.class,
                        (f, s) -> f.thenApply(s::a).thenApply(s::b));
                engine.register(spec.definition());
                engine.start(spec.name(), spec.version(), Map.of(), null);

                TaskActivation t = engine.poll("jw", Set.of(), null, 1, 5_000L,
                        System.currentTimeMillis() + 5_000, () -> false).get(0);
                engine.complete(t.taskId(), t.leaseOwner(), Map.of("done", "a"));

                Token settled = storage.inTx(tx -> tx.findToken(t.taskId())).orElseThrow();
                assertSame(TokenStatus.DONE, settled.status,
                        "the settle was durable before complete() returned");
                boolean continuationMirrored = storage.inTx(tx -> tx.tokensOf(settled.instanceId)).stream()
                        .anyMatch(x -> x.status == TokenStatus.READY);
                assertTrue(continuationMirrored, "the continuation's INSERT was in the same flush");
            } finally {
                engine.close();
            }
        }
    }

    @Test @DisplayName("un-acked work is invisible in the DB; close() drains it")
    void unackedInvisibleAndCloseDrains() throws Exception {
        try (Storage storage = h2("journal-drain")) {
            String instanceId = seedInstance(storage, "drain-wf");
            Journal j = new Journal(storage, new DispatchNotifier(), new ThreadLocal<>(),
                    "owner-a", 60_000 /* linger: park the flush */, 1_000_000, 5_000, 1_000);
            assertTrue(j.active());

            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread op = new Thread(() -> {
                try {
                    j.run(tx -> {
                        Instance i = tx.lockInstance(instanceId).orElseThrow();
                        i.contextJson = "{\"marker\":1}";
                        tx.updateInstance(i);
                        return null;
                    });
                } catch (Throwable t) {
                    failure.set(t);
                }
            });
            op.start();
            Thread.sleep(300);
            assertTrue(op.isAlive(), "the op parks on its ack until a flush");
            String mirrored = storage.inTx(tx -> tx.findInstance(instanceId)).orElseThrow().contextJson;
            assertFalse(mirrored.contains("marker"), "un-acked writes never reach the database");

            j.close();                                   // drains the pending cycle, then releases
            op.join(10_000);
            assertNull(failure.get());
            mirrored = storage.inTx(tx -> tx.findInstance(instanceId)).orElseThrow().contextJson;
            assertTrue(mirrored.contains("marker"), "close() flushed what was pending");
        }
    }

    @Test @DisplayName("one writer per database: a second journal refuses the data plane")
    void leaseIsExclusive() throws Exception {
        try (Storage storage = h2("journal-excl")) {
            Journal first = new Journal(storage, new DispatchNotifier(), new ThreadLocal<>(),
                    "owner-a", 5, 256, 10_000, 1_000);
            try {
                assertTrue(first.active());
                Journal second = new Journal(storage, new DispatchNotifier(), new ThreadLocal<>(),
                        "owner-b", 5, 256, 10_000, 1_000);
                assertFalse(second.active(), "the live lease is not claimable");
                EngineException e = assertThrows(EngineException.class,
                        () -> second.run(tx -> null));
                assertTrue(e.getMessage().contains("journal inactive"), e.getMessage());
                second.close();
            } finally {
                first.close();
            }
        }
    }

    @Test @DisplayName("a fenced-out zombie demotes: its flush aborts, its callers fail, nothing corrupts")
    void fencedZombieDemotes() throws Exception {
        try (Storage storage = h2("journal-fence")) {
            String instanceId = seedInstance(storage, "fence-wf");
            Journal zombie = new Journal(storage, new DispatchNotifier(), new ThreadLocal<>(),
                    "owner-a", 5, 256, 10_000, 1_000);
            assertTrue(zombie.active());

            // Ownership moves while the zombie is "paused": release its lease and claim a new one.
            long stolenGen = storage.inTx(tx -> {
                tx.releaseJournalLease("owner-a", zombie.generation());
                return tx.claimJournalLease("thief", 60_000);
            });
            assertTrue(stolenGen > zombie.generation());

            EngineException e = assertThrows(EngineException.class, () -> zombie.run(tx -> {
                Instance i = tx.lockInstance(instanceId).orElseThrow();
                i.contextJson = "{\"zombie\":true}";
                tx.updateInstance(i);
                return null;
            }));
            assertTrue(e.getMessage().contains("journal"), e.getMessage());
            assertFalse(zombie.active(), "the failed fence demoted it");
            String mirrored = storage.inTx(tx -> tx.findInstance(instanceId)).orElseThrow().contextJson;
            assertFalse(mirrored.contains("zombie"), "the aborted flush left no trace");
            assertThrows(EngineException.class, () -> zombie.run(tx -> null),
                    "a demoted journal refuses everything after");
            zombie.close();
        }
    }

    @Test @DisplayName("two engines on one database: only the first serves; the second is inert")
    void secondEngineOnSameDbIsInert() {
        try (Storage storage = h2("journal-2engine")) {
            WorkflowEngine first = new WorkflowEngine(storage, new DefinitionRegistry(storage, 256),
                    20_000, () -> "wfi_1" + System.nanoTime(), true);
            WorkflowEngine second = new WorkflowEngine(storage, new DefinitionRegistry(storage, 256),
                    20_000, () -> "wfi_2" + System.nanoTime(), true);
            try {
                assertTrue(first.journal.active(), "first claims the lease");
                assertFalse(second.journal.active(), "journal mode is one writer per database");
            } finally {
                first.close();
                second.close();
            }
        }
    }

    private static String seedInstance(Storage storage, String workflow) {
        String id = "wfi_seed" + System.nanoTime();
        storage.inTxVoid(tx -> {
            Instance i = new Instance();
            i.id = id;
            i.workflow = workflow;
            i.version = 1;
            i.contextJson = "{}";
            i.createdAt = System.currentTimeMillis();
            i.updatedAt = i.createdAt;
            tx.insertInstance(i);
            Token t = new Token();
            t.id = "tok_seed" + System.nanoTime();
            t.instanceId = id;
            t.workflow = workflow;
            t.version = 1;
            t.nodeId = "n1";
            t.kind = NodeKind.TASK;
            t.status = TokenStatus.READY;
            t.availableAt = System.currentTimeMillis();
            t.joinStack = "";
            t.updatedAt = System.currentTimeMillis();
            tx.insertToken(t);
        });
        return id;
    }
}
