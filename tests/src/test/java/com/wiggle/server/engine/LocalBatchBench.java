package com.wiggle.server.engine;

import com.wiggle.core.ExecutionMode;
import com.wiggle.core.Node;
import com.wiggle.core.TaskActivation;
import com.wiggle.core.WorkflowDefinition;
import com.wiggle.server.store.InMemoryStorage;
import com.wiggle.server.store.Storage;
import com.wiggle.server.store.Tx;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * What a LOCAL_ASYNC batch costs the DATABASE, as distinct from what it saves on the wire.
 *
 * <p>A worker buffers up to {@code localBatchSize} steps and reports them in one {@code advanceRun}
 * call, so the client-to-server round trips collapse from K to 1. The question this asks is what
 * happens next: {@code doAdvance} walks the batch a step at a time, and the engine never reads
 * {@link ExecutionMode} at all -- LOCAL_SYNC and LOCAL_ASYNC take byte-for-byte the same server
 * path, differing only in how many steps arrive per call.
 *
 * <p>If the per-step writes are unchanged by batching, statements-per-step is flat across K and the
 * batch saves nothing below the RPC. The floor for comparison is what a run of K steps MUST leave
 * behind to be resumable: the entry token settled, one continuation written, one instance touched.
 *
 * <p>{@code WIGGLE_BENCH_LOCAL=1 ./gradlew :tests:test --tests '*LocalBatchBench*' -i}
 */
@EnabledIfEnvironmentVariable(named = "WIGGLE_BENCH_LOCAL", matches = ".+")
class LocalBatchBench {

    private static final int[] BATCHES = {1, 2, 4, 8, 16, 32, 64};
    private static final String QUEUE = "q";

    private record Tally(int steps, Map<String, Long> byMethod, long total) {
        double perStep() { return (double) total / steps; }
        long of(String m) { return byMethod.getOrDefault(m, 0L); }
    }

    @Test
    @DisplayName("database statements per step, across LOCAL_ASYNC batch size")
    void statementsPerStep() {
        List<Tally> results = new ArrayList<>();
        for (int k : BATCHES) results.add(run(k));

        System.out.println();
        System.out.printf("%-8s %-12s %-12s %-14s %-12s %-12s%n",
                "batch", "updateToken", "insertToken", "updateInstance", "total", "per step");
        System.out.println("-".repeat(76));
        for (Tally t : results) {
            System.out.printf("%-8d %-12d %-12d %-14d %-12d %-12.1f%n",
                    t.steps(), t.of("updateToken"), t.of("insertToken"),
                    t.of("updateInstance"), t.total(), t.perStep());
        }
        System.out.println();
        System.out.println("floor for a resumable run of K steps: 1 settle + 1 insert + 1 touch = 3, any K");
        Tally last = results.getLast();
        System.out.printf("at batch %d: %d statements where 3 would do -- %.0fx%n",
                last.steps(), last.total(), last.total() / 3.0);
    }

    /** Claims the head of a K-step chain and reports the whole run in one advance call. */
    private static Tally run(int steps) {
        Map<String, Long> counts = new ConcurrentHashMap<>();
        AtomicLong total = new AtomicLong();
        try (Storage raw = new InMemoryStorage()) {
            raw.migrate();
            Storage counting = new Counting(raw, counts, total);
            DefinitionRegistry registry = new DefinitionRegistry(counting);
            registry.register(chain(steps));
            WorkflowEngine engine = new WorkflowEngine(counting, registry, 60_000);

            engine.start("chain", 1, Map.of("n", 0L), null);
            TaskActivation head = engine.poll("w", Set.of(QUEUE), 1, 60_000L).getFirst();

            counts.clear();
            total.set(0);   // measure the advance alone, not the start or the claim

            List<WorkflowEngine.StepInput> run = new ArrayList<>(steps);
            for (int i = 0; i < steps; i++) {
                run.add(new WorkflowEngine.StepInput("n" + i, Map.of("n", (long) i + 1), null));
            }
            engine.advance(head.taskId(), head.leaseOwner(), run, false);
        }
        return new Tally(steps, Map.copyOf(counts), total.get());
    }

    /** A Storage whose every transaction hands out a statement-counting {@link Tx}. */
    private record Counting(Storage delegate, Map<String, Long> counts, AtomicLong total)
            implements Storage {

        @Override public void migrate() { delegate.migrate(); }

        @Override public <R> R inTx(Function<Tx, R> work) {
            return delegate.inTx(tx -> work.apply(watch(tx, counts, total)));
        }

        @Override public void close() { delegate.close(); }
    }

    /** Counts the mutating calls -- what a JDBC backend turns into statements. */
    private static Tx watch(Tx delegate, Map<String, Long> counts, AtomicLong total) {
        Set<String> writes = Set.of("updateToken", "insertToken", "updateInstance",
                "insertInstance", "appendCompensation", "markCompensated");
        InvocationHandler h = (proxy, method, args) -> {
            Object out = method.invoke(delegate, args);
            if (writes.contains(method.getName())) {
                counts.merge(method.getName(), 1L, Long::sum);
                total.incrementAndGet();
            }
            return out;
        };
        return (Tx) Proxy.newProxyInstance(Tx.class.getClassLoader(), new Class<?>[]{Tx.class}, h);
    }

    /** n0 -> n1 -> ... -> n(steps-1) -> end, every node a task on one queue. */
    private static WorkflowDefinition chain(int steps) {
        Map<String, Node> n = new LinkedHashMap<>();
        for (int i = 0; i < steps; i++) {
            String next = i == steps - 1 ? "end" : "n" + (i + 1);
            n.put("n" + i, Node.task("n" + i, "n" + i, "act", QUEUE, null).withNext(next));
        }
        n.put("end", Node.end("end", true, null));
        return new WorkflowDefinition("chain", 1, "n0", n, Set.of(QUEUE),
                ExecutionMode.LOCAL_ASYNC);
    }
}
