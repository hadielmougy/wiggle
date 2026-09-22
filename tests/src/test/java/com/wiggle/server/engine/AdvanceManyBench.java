package com.wiggle.server.engine;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.core.ExecutionMode;
import com.wiggle.core.TaskActivation;
import com.wiggle.jdbc.JdbcStorage;
import com.wiggle.postgres.PostgresDialect;
import com.wiggle.server.engine.WorkflowEngine.Run;
import com.wiggle.server.engine.WorkflowEngine.RunResult;
import com.wiggle.server.engine.WorkflowEngine.StepInput;
import com.wiggle.server.store.Storage;
import com.wiggle.tests.TestDb;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The ceiling question from docs' cross-instance batching plan: with N runs in one call, what does
 * the report path actually gain over N single calls? Timed against a real PostgreSQL, because the
 * lever under test is transactions and commits -- an in-memory store would measure map lookups and
 * say nothing.
 *
 * <p>What is measured is the report phase alone -- starts and polls happen off the clock -- on one
 * thread, so the comparison isolates per-call cost: batch size 1 is exactly today's
 * {@code advance}, and every other row is the same work through {@code advanceMany}. The RPC
 * saving is not in these numbers at all ({@code advanceMany} has no wire surface yet); whatever
 * shows here is the transaction-and-commit lever alone.
 *
 * <pre>
 *   WIGGLE_BENCH_ADVANCE=1 \
 *   WIGGLE_TEST_PG_URL=jdbc:postgresql://localhost:55432/wiggle \
 *   WIGGLE_TEST_PG_USER=wiggle WIGGLE_TEST_PG_PASSWORD=wiggle \
 *     ./gradlew :tests:test --tests "*AdvanceManyBench*" -i
 * </pre>
 */
@EnabledIfEnvironmentVariable(named = "WIGGLE_BENCH_ADVANCE", matches = ".+")
@EnabledIfEnvironmentVariable(named = "WIGGLE_TEST_PG_URL", matches = ".+")
class AdvanceManyBench {

    private static final int INSTANCES = 256;
    private static final int STEPS = 3;
    private static final int[] BATCH = {1, 8, 16, 32, 64};

    /** The steps this spec names; a worker binds them by name. */
    interface ThreeSteps {
        Map<String, Object> x(Map<String, Object> ctx);
        Map<String, Object> y(Map<String, Object> ctx);
        Map<String, Object> z(Map<String, Object> ctx);
    }

    private record Row(int batch, long millis) {
        double runsPerSec() { return INSTANCES * 1000.0 / millis; }
        double stepsPerSec() { return INSTANCES * STEPS * 1000.0 / millis; }
        double msPerRun() { return (double) millis / INSTANCES; }
    }

    @Test
    @DisplayName("report-path ceiling: single advance vs advanceMany, across batch size")
    void reportPathCeiling() {
        try (Storage storage = new JdbcStorage(TestDb.url("PG"),
                TestDb.user("PG"), TestDb.password("PG"), 4, new PostgresDialect())) {
            storage.migrate();
            DefinitionRegistry registry = new DefinitionRegistry(storage);
            WorkflowEngine engine = new WorkflowEngine(storage, registry, 60_000);
            FlowSpec bp = FlowSpec.define("amb-chain", 1, Map.class, ThreeSteps.class, (f, s) -> f
                    .executeInLocalAsync()
                    .thenApply(s::x)
                    .thenApply(s::y)
                    .thenApply(s::z));
            registry.register(bp.definition());

            time(engine, bp, 16, 64);   // warmup: JIT, connection pool, planner -- off the books

            // Best of three rounds per size: this is a ceiling, and a single sample is one
            // autovacuum or checkpoint away from measuring the database's mood instead.
            Map<Integer, Long> best = new LinkedHashMap<>();
            for (int round = 0; round < 3; round++) {
                for (int batch : BATCH) {
                    long ms = time(engine, bp, batch, INSTANCES);
                    best.merge(batch, ms, Math::min);
                }
            }
            List<Row> rows = new ArrayList<>();
            best.forEach((batch, ms) -> rows.add(new Row(batch, ms)));

            System.out.println();
            System.out.printf("%d instances x %d steps, one thread, report phase only%n", INSTANCES, STEPS);
            System.out.printf("%-8s %-10s %-10s %-12s %-10s%n", "batch", "ms", "runs/s", "steps/s", "ms/run");
            System.out.println("-".repeat(52));
            for (Row r : rows) {
                System.out.printf("%-8d %-10d %-10.0f %-12.0f %-10.2f%n",
                        r.batch(), r.millis(), r.runsPerSec(), r.stepsPerSec(), r.msPerRun());
            }
            Row single = rows.getFirst();
            Row widest = rows.getLast();
            System.out.printf("%nat batch %d: %.1fx the single-call ceiling%n",
                    widest.batch(), (double) single.millis() / widest.millis());
        }
    }

    /** Starts {@code n} instances, claims their first tasks, then times only the reports. */
    private static long time(WorkflowEngine engine, FlowSpec bp, int batch, int n) {
        Set<String> queues = bp.definition().queues();
        for (int i = 0; i < n; i++) engine.start(bp.name(), bp.version(), Map.of(), null);
        List<TaskActivation> claimed = new ArrayList<>();
        while (claimed.size() < n) {
            List<TaskActivation> got = engine.poll("bench-w", queues, n - claimed.size(), null);
            assertTrue(!got.isEmpty(), "claimed " + claimed.size() + " of " + n);
            claimed.addAll(got);
        }
        List<Run> runs = claimed.stream().map(t -> fullRun(bp, t)).toList();

        long begin = System.nanoTime();
        int completed = 0;
        if (batch == 1) {
            for (Run run : runs) {
                if ("COMPLETED".equals(engine.advance(run.startTaskId(), run.leaseOwner(),
                        run.steps(), run.finalHandback()).instanceStatus())) completed++;
            }
        } else {
            for (int i = 0; i < runs.size(); i += batch) {
                Map<String, RunResult> results =
                        engine.advanceMany(runs.subList(i, Math.min(i + batch, runs.size())));
                for (RunResult r : results.values()) {
                    if (r.ok() && "COMPLETED".equals(r.outcome().instanceStatus())) completed++;
                }
            }
        }
        long millis = (System.nanoTime() - begin) / 1_000_000;
        assertEquals(n, completed, "every run must land, or the numbers measure failures");
        return millis;
    }

    /** The whole three-step run, exactly as a LOCAL_ASYNC worker would flush it. */
    private static Run fullRun(FlowSpec bp, TaskActivation first) {
        String x = first.nodeId();
        String y = bp.definition().node(x).next();
        String z = bp.definition().node(y).next();
        return new Run(first.taskId(), "bench-w", List.of(
                new StepInput(x, Map.of("x", 1L), null),
                new StepInput(y, Map.of("x", 1L, "y", 2L), null),
                new StepInput(z, Map.of("x", 1L, "y", 2L, "z", 3L), null)), true);
    }
}
