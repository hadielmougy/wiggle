package com.wiggle.tests;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.flow.Wiggle;
import com.wiggle.core.InstanceView;
import com.wiggle.core.Json;
import com.wiggle.core.TaskActivation;
import com.wiggle.jdbc.JdbcStorage;
import com.wiggle.postgres.H2Dialect;
import com.wiggle.server.engine.DefinitionRegistry;
import com.wiggle.server.engine.ReportOutcome;
import com.wiggle.server.engine.WorkflowEngine;
import com.wiggle.server.store.InMemoryStorage;
import com.wiggle.server.store.Storage;
import com.wiggle.server.store.Tx;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Two arms of one fork reported at once: the second arrives while the first holds the instance
 * lock, joins its group, and both commit in the first one's transaction -- the join fires, and the
 * second never waits on the instance lock of its own.
 */
class SiblingReportsTest {

    interface ForkSteps {
        Map<String, Object> a(Map<String, Object> ctx);
        Map<String, Object> b(Map<String, Object> ctx);
        Map<String, Object> pick(Map<String, Object> left, Map<String, Object> right);
    }

    /** Counts the transactions that lock a task, and holds the first lock until released. */
    static final class Gate {
        final CountDownLatch locked = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicInteger lockingTransactions = new AtomicInteger();
        final AtomicInteger locks = new AtomicInteger();
        volatile boolean armed;

        Storage wrap(Storage storage) {
            return (Storage) Proxy.newProxyInstance(Storage.class.getClassLoader(), new Class<?>[]{Storage.class},
                    (proxy, method, args) -> {
                        if (args != null) {
                            for (int i = 0; i < args.length; i++) {
                                if (args[i] instanceof Function<?, ?> work) args[i] = wrapWork(work);
                            }
                        }
                        try {
                            return method.invoke(storage, args);
                        } catch (InvocationTargetException e) {
                            throw e.getCause();
                        }
                    });
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        private Function wrapWork(Function work) {
            return raw -> raw instanceof Tx tx ? work.apply(wrapTx(tx)) : work.apply(raw);
        }

        private Tx wrapTx(Tx tx) {
            boolean[] counted = {false};
            return (Tx) Proxy.newProxyInstance(Tx.class.getClassLoader(), new Class<?>[]{Tx.class},
                    (proxy, method, args) -> {
                        if (method.getName().equals("lockTask")) {
                            locks.incrementAndGet();
                            if (!counted[0]) {
                                counted[0] = true;
                                lockingTransactions.incrementAndGet();
                            }
                            if (armed) {
                                armed = false;
                                locked.countDown();
                                release.await();
                            }
                        }
                        try {
                            return method.invoke(tx, args);
                        } catch (InvocationTargetException e) {
                            throw e.getCause();
                        }
                    });
        }
    }

    private static Storage storage(String kind) {
        return switch (kind) {
            case "memory" -> new InMemoryStorage();
            case "h2" -> new JdbcStorage("jdbc:h2:mem:siblings-" + System.nanoTime()
                    + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1", "sa", "", 8, new H2Dialect());
            default -> throw new IllegalArgumentException(kind);
        };
    }

    @ParameterizedTest(name = "{0}") @ValueSource(strings = {"memory", "h2"}) @Timeout(30)
    @DisplayName("an arm reported while its sibling holds the lock commits in the sibling's transaction")
    void armsShareOneTransaction(String kind) throws Exception {
        FlowSpec bp = FlowSpec.define("siblings", 1, Map.class, ForkSteps.class, (f, s) ->
                Wiggle.allOf(f.thenApply(s::a), f.thenApply(s::b))
                        .combine(s::pick));
        Gate gate = new Gate();
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try (Storage raw = storage(kind)) {
            raw.migrate();
            Storage storage = gate.wrap(raw);
            DefinitionRegistry registry = new DefinitionRegistry(storage);
            WorkflowEngine engine = new WorkflowEngine(storage, registry, 30_000);
            registry.register(bp.definition());
            String id = engine.start(bp.name(), bp.version(), Map.of(), null);
            List<TaskActivation> arms = engine.poll("w", bp.definition().queues(), 10, null);
            assertEquals(2, arms.size(), "both arms leased");

            int before = gate.lockingTransactions.get();
            gate.armed = true;
            Future<ReportOutcome> first = pool.submit(() -> engine.report(run(arms.get(0))));
            gate.locked.await();
            Thread[] follower = new Thread[1];
            Future<ReportOutcome> second = pool.submit(() -> {
                follower[0] = Thread.currentThread();
                return engine.report(run(arms.get(1)));
            });
            while (follower[0] == null || follower[0].getState() != Thread.State.WAITING) Thread.onSpinWait();
            gate.release.countDown();

            assertEquals("RUNNING", first.get().instanceStatus());
            assertEquals("RUNNING", second.get().instanceStatus());
            assertEquals(1, gate.lockingTransactions.get() - before, "both arms applied in one transaction");

            List<TaskActivation> pick = engine.poll("w", bp.definition().queues(), 10, null);
            assertEquals(1, pick.size(), "the join fired once");
            Map<String, Object> merged = new LinkedHashMap<>();
            merged.put("a", 1L);
            merged.put("b", 2L);
            engine.report(new WorkflowEngine.Run(pick.getFirst().taskId(), pick.getFirst().leaseOwner(),
                    List.of(new WorkflowEngine.StepInput(pick.getFirst().nodeId(), merged, null)), true));
            InstanceView view = engine.instance(id).orElseThrow();
            assertEquals("COMPLETED", view.status());
            assertEquals(merged, Json.asObject(view.context()));
        } finally {
            pool.shutdownNow();
        }
    }

    @ParameterizedTest(name = "{0}") @ValueSource(strings = {"memory", "h2"}) @Timeout(30)
    @DisplayName("a sibling whose lease does not check out reports on its own, and the leader still commits")
    void badSiblingReportsAlone(String kind) throws Exception {
        FlowSpec bp = FlowSpec.define("siblings-bad", 1, Map.class, ForkSteps.class, (f, s) ->
                Wiggle.allOf(f.thenApply(s::a), f.thenApply(s::b))
                        .combine(s::pick));
        Gate gate = new Gate();
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try (Storage raw = storage(kind)) {
            raw.migrate();
            Storage storage = gate.wrap(raw);
            DefinitionRegistry registry = new DefinitionRegistry(storage);
            WorkflowEngine engine = new WorkflowEngine(storage, registry, 30_000);
            registry.register(bp.definition());
            engine.start(bp.name(), bp.version(), Map.of(), null);
            List<TaskActivation> arms = engine.poll("w", bp.definition().queues(), 10, null);

            gate.armed = true;
            Future<ReportOutcome> first = pool.submit(() -> engine.report(run(arms.get(0))));
            gate.locked.await();
            Thread[] follower = new Thread[1];
            TaskActivation other = arms.get(1);
            Future<ReportOutcome> second = pool.submit(() -> {
                follower[0] = Thread.currentThread();
                return engine.report(new WorkflowEngine.Run(other.taskId(), "someone-else",
                        List.of(new WorkflowEngine.StepInput(other.nodeId(), Map.of(), null)), true));
            });
            while (follower[0] == null || follower[0].getState() != Thread.State.WAITING) Thread.onSpinWait();
            gate.release.countDown();

            assertEquals("RUNNING", first.get().instanceStatus(), "the leader's own run committed");
            Exception refused = assertThrows(Exception.class, second::get);
            assertTrue(String.valueOf(refused.getCause()).contains("lease"), String.valueOf(refused.getCause()));
            ReportOutcome retried = engine.report(run(other));
            assertEquals("RUNNING", retried.instanceStatus(), "the refused arm reports fine with its own lease");
            assertEquals(1, engine.poll("w", bp.definition().queues(), 10, null).size(), "the join fired");
        } finally {
            pool.shutdownNow();
        }
    }

    private static WorkflowEngine.Run run(TaskActivation t) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put(t.stepName().equals("a") ? "a" : "b", t.stepName().equals("a") ? 1L : 2L);
        return new WorkflowEngine.Run(t.taskId(), t.leaseOwner(),
                List.of(new WorkflowEngine.StepInput(t.nodeId(), out, null)), true);
    }
}
