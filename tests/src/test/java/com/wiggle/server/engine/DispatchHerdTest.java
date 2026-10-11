package com.wiggle.server.engine;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.core.Ids;
import com.wiggle.core.TaskActivation;
import com.wiggle.server.store.InMemoryStorage;
import com.wiggle.server.store.Storage;
import com.wiggle.server.store.Tx;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Parked long-polls on one node, counted where it costs: the claim queries that reach the store.
 * Many polls for the same work must not each claim on every wake or every fallback interval.
 */
class DispatchHerdTest {

    interface OneStep {
        Map<String, Object> work(Map<String, Object> ctx);
    }

    /** {@link InMemoryStorage} that counts {@code claimTasks} calls. */
    private static final class CountingStorage implements Storage {
        final InMemoryStorage delegate = new InMemoryStorage();
        final AtomicInteger claims = new AtomicInteger();
        final Map<String, AtomicInteger> claimsBy = new java.util.concurrent.ConcurrentHashMap<>();

        @Override public void migrate() { delegate.migrate(); }

        @Override public <R> R inTx(Function<Tx, R> work) {
            return delegate.inTx(tx -> work.apply((Tx) Proxy.newProxyInstance(Tx.class.getClassLoader(),
                    new Class<?>[]{Tx.class}, (proxy, method, args) -> {
                        if (method.getName().equals("claimTasks")) {
                            claims.incrementAndGet();
                            claimsBy.computeIfAbsent((String) args[0], k -> new AtomicInteger()).incrementAndGet();
                        }
                        try {
                            return method.invoke(tx, args);
                        } catch (InvocationTargetException e) {
                            throw e.getCause();
                        }
                    })));
        }

        @Override public void close() { delegate.close(); }
    }

    private static final int POLLS = 8;

    private static List<Future<List<TaskActivation>>> park(ExecutorService pool, WorkflowEngine engine,
                                                          Set<String> queues, long millis) {
        long deadline = System.currentTimeMillis() + millis;
        List<Future<List<TaskActivation>>> polls = new ArrayList<>();
        for (int i = 0; i < POLLS; i++) {
            String worker = "w" + i;
            polls.add(pool.submit(() -> engine.poll(worker, queues, null, 1, null, deadline, Cancellation.never())));
        }
        return polls;
    }

    @Test @DisplayName("idle parked polls make one fallback claim per interval between them, not one each")
    void idlePollsShareTheFallback() throws Exception {
        try (CountingStorage storage = new CountingStorage()) {
            storage.migrate();
            WorkflowEngine engine = new WorkflowEngine(storage, new DefinitionRegistry(storage), 30_000);
            FlowSpec bp = FlowSpec.define("herd-" + Ids.next("wf"), 1, Map.class, OneStep.class,
                    (f, s) -> f.thenApply(s::work));
            engine.register(bp.definition());
            ExecutorService pool = Executors.newCachedThreadPool();
            try {
                for (Future<List<TaskActivation>> p : park(pool, engine, bp.definition().workerQueues(), 1_000)) {
                    assertTrue(p.get(5, TimeUnit.SECONDS).isEmpty());
                }
            } finally {
                pool.shutdownNow();
            }
            // Each poll claims on arrival and once at its deadline; in between, one fallback claim
            // every 100ms for the whole line. Eight polls claiming every 100ms each would be ~90.
            int claims = storage.claims.get();
            assertTrue(claims <= 2 * POLLS + 15, "claims over one idle second: " + claims);
        }
    }

    @Test @DisplayName("one produced task is claimed by one parked poll, without waking the rest")
    void oneTaskWakesOnePoll() throws Exception {
        try (CountingStorage storage = new CountingStorage()) {
            storage.migrate();
            WorkflowEngine engine = new WorkflowEngine(storage, new DefinitionRegistry(storage), 30_000);
            FlowSpec bp = FlowSpec.define("herd-" + Ids.next("wf"), 1, Map.class, OneStep.class,
                    (f, s) -> f.thenApply(s::work));
            engine.register(bp.definition());
            ExecutorService pool = Executors.newCachedThreadPool();
            try {
                List<Future<List<TaskActivation>>> polls = park(pool, engine, bp.definition().workerQueues(), 3_000);
                Thread.sleep(50);   // every poll has made its arrival claim and parked
                int before = storage.claims.get();

                engine.start(bp.name(), bp.version(), Map.of(), null);
                Future<List<TaskActivation>> winner = null;
                long until = System.currentTimeMillis() + 2_000;
                while (winner == null && System.currentTimeMillis() < until) {
                    for (Future<List<TaskActivation>> p : polls) if (p.isDone()) winner = p;
                    Thread.sleep(2);
                }
                assertNotNull(winner, "a parked poll was woken and claimed the task");
                assertEquals(1, winner.get().size());
                int woken = storage.claims.get() - before;
                // The woken poll's claim, plus at most the line front's fallback if it fell due.
                assertTrue(woken <= 2, "claims to hand out one task: " + woken);
                Thread.sleep(50);
                assertEquals(1, polls.stream().filter(Future::isDone).count(),
                        "a signalled claim that came back full did not wake another poll");
                assertEquals(1, polls.stream().filter(Future::isDone).count(), "the other polls stayed parked");
            } finally {
                pool.shutdownNow();
            }
        }
    }

    @Test @DisplayName("#293: an idle version-scoped poll ignores other versions' work and backs off its fallback")
    void idleScopedPollStaysQuiet() throws Exception {
        try (CountingStorage storage = new CountingStorage()) {
            storage.migrate();
            WorkflowEngine engine = new WorkflowEngine(storage, new DefinitionRegistry(storage), 30_000);
            String name = "herd-" + Ids.next("wf");
            FlowSpec v1 = FlowSpec.define(name, 1, Map.class, OneStep.class, (f, s) -> f.thenApply(s::work));
            FlowSpec v2 = FlowSpec.define(name, 2, Map.class, OneStep.class, (f, s) -> f.thenApply(s::work));
            engine.register(v1.definition());
            engine.register(v2.definition());
            Set<String> queues = v1.definition().workerQueues();
            ExecutorService pool = Executors.newCachedThreadPool();
            try {
                long deadline = System.currentTimeMillis() + 2_500;
                Future<List<TaskActivation>> scoped = pool.submit(() -> engine.poll("scoped", queues,
                        Set.of(new com.wiggle.core.WorkflowVersion(name, 2)), 1, null, deadline, Cancellation.never()));
                for (int i = 0; i < 100; i++) {   // v1 work keeps arriving on the same queue
                    engine.start(name, 1, Map.of(), null);
                    Thread.sleep(20);
                }
                int idle = storage.claimsBy.getOrDefault("scoped", new AtomicInteger()).get();
                // Arrival, then fallbacks at 100, 300, 700 and 1500 ms. Woken by every v1 task, it
                // would have claimed about a hundred times.
                assertTrue(idle <= 8, "claims by an idle scoped poll over two busy seconds: " + idle);

                long t0 = System.nanoTime();
                engine.start(name, 2, Map.of(), null);
                List<TaskActivation> got = scoped.get(5, TimeUnit.SECONDS);
                long ms = (System.nanoTime() - t0) / 1_000_000;
                assertEquals(1, got.size(), "its own version's task reaches it");
                assertEquals(2, got.getFirst().version());
                assertTrue(ms < 300, "on the wake, not a backed-off fallback: " + ms + " ms");
            } finally {
                pool.shutdownNow();
            }
        }
    }
}
