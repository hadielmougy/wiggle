package com.wiggle.server.engine;

import com.wiggle.tests.Reports;
import com.wiggle.core.Node;
import com.wiggle.core.TaskActivation;
import com.wiggle.core.WorkflowDefinition;
import com.wiggle.server.store.InMemoryStorage;
import com.wiggle.server.store.PayloadCodec;
import com.wiggle.server.store.Rows.Token;
import com.wiggle.server.store.Storage;
import com.wiggle.server.store.Tx;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * What fork WIDTH costs in rows read and payload bytes materialised, as distinct from what nesting
 * DEPTH costs in bytes written (see {@code NestingPayloadBench}).
 *
 * <p>The suspicion under test: {@code tokensOf(instanceId)} returns every token of an instance and
 * every backend's row mapper decodes each payload as it goes, so a join that calls it once per
 * arriving branch reads the whole fan-out W times over. If that is what happens, rows-read grows
 * as W squared while the flow itself is linear, and every one of those rows carries a full copy of
 * the pre-fork view.
 *
 * <p>Counted, not timed: the proxy below records what the engine ASKS the store for, which is the
 * same on every backend. Wall-clock would only measure the in-memory store's map lookups and would
 * say nothing about the JSON parse a real database pays. Bytes are computed by re-encoding what was
 * handed back, which is exactly what a JDBC row mapper decodes.
 *
 * <p>{@code WIGGLE_BENCH_FORK=1 ./gradlew :tests:test --tests '*ForkWidthBench*' -i}
 */
@EnabledIfEnvironmentVariable(named = "WIGGLE_BENCH_FORK", matches = ".+")
class ForkWidthBench {

    private static final int[] WIDTHS = {2, 4, 8, 16, 32, 64, 128};
    private static final String QUEUE = "q";

    /** Context filler, so a view is a realistic size rather than a handful of bytes. */
    private static final int BALLAST = 400;

    private record Tally(int width, long tokensOfCalls, long rowsRead, long payloadBytesRead) {
        double rowsPerBranch() { return (double) rowsRead / width; }
        double kbRead() { return payloadBytesRead / 1024.0; }
    }

    @Test
    @DisplayName("rows and payload bytes a join reads, across fork width")
    void forkWidthCost() {
        List<Tally> results = new ArrayList<>();
        for (int w : WIDTHS) results.add(run(w));

        System.out.println();
        System.out.printf("%-8s %-14s %-12s %-14s %-12s%n",
                "width", "tokensOf()", "rows read", "rows/branch", "payload KB");
        System.out.println("-".repeat(64));
        for (Tally t : results) {
            System.out.printf("%-8d %-14d %-12d %-14.1f %-12.1f%n",
                    t.width(), t.tokensOfCalls(), t.rowsRead(), t.rowsPerBranch(), t.kbRead());
        }
        System.out.println();
        Tally first = results.getFirst(), last = results.getLast();
        System.out.printf("width x%.0f  ->  rows x%.0f,  bytes x%.0f%n",
                (double) last.width() / first.width(),
                (double) last.rowsRead() / Math.max(1, first.rowsRead()),
                (double) last.payloadBytesRead() / Math.max(1, first.payloadBytesRead()));
        System.out.println("linear in width would be the same factor; quadratic is its square.");
    }

    /** Drives one fork/join instance of the given width to completion, counting store reads. */
    private static Tally run(int width) {
        AtomicLong calls = new AtomicLong(), rows = new AtomicLong(), bytes = new AtomicLong();
        try (Storage raw = new InMemoryStorage()) {
            raw.migrate();
            Storage counting = new Counting(raw, calls, rows, bytes);
            DefinitionRegistry registry = new DefinitionRegistry(counting);
            registry.register(forkOf(width));
            WorkflowEngine engine = new WorkflowEngine(counting, registry, 60_000);

            engine.start("fan", 1, ballast(), null);
            // Every branch is one task; completing them all drives the join.
            for (int i = 0; i < width; i++) {
                List<TaskActivation> claimed = engine.poll("w", Set.of(QUEUE), 1, 60_000L);
                if (claimed.isEmpty()) throw new IllegalStateException("nothing dispatchable at branch " + i);
                TaskActivation t = claimed.getFirst();
                Reports.one(engine, t, ballast());
            }
        }
        return new Tally(width, calls.get(), rows.get(), bytes.get());
    }

    /** A Storage whose every transaction hands out a counting {@link Tx}. */
    private record Counting(Storage delegate, AtomicLong calls, AtomicLong rows, AtomicLong bytes)
            implements Storage {

        @Override public void migrate() { delegate.migrate(); }

        @Override public <R> R inTx(Function<Tx, R> work) {
            return delegate.inTx(tx -> work.apply(watch(tx, calls, rows, bytes)));
        }

        @Override public void close() { delegate.close(); }
    }

    private static Tx watch(Tx delegate, AtomicLong calls, AtomicLong rows, AtomicLong bytes) {
        InvocationHandler h = (proxy, method, args) -> {
            Object out = method.invoke(delegate, args);
            if (method.getName().equals("tokensOf")) {
                calls.incrementAndGet();
                @SuppressWarnings("unchecked") List<Token> ts = (List<Token>) out;
                rows.addAndGet(ts.size());
                for (Token t : ts) {
                    String encoded = PayloadCodec.encode(t.payload);
                    if (encoded != null) bytes.addAndGet(encoded.length());
                }
            }
            return out;
        };
        return (Tx) Proxy.newProxyInstance(Tx.class.getClassLoader(), new Class<?>[]{Tx.class}, h);
    }

    /** fork -> width branches, each one task -> join -> end. No combine: this measures the barrier. */
    private static WorkflowDefinition forkOf(int width) {
        Map<String, Node> n = new LinkedHashMap<>();
        List<String> branches = new ArrayList<>(width);
        for (int i = 0; i < width; i++) {
            String id = "b" + i;
            branches.add(id);
            n.put(id, Node.task(id, id, id, QUEUE, null).withNext("join"));
        }
        n.put("fork", Node.fork("fork", "fork").withBranches(branches).withNext("join"));
        n.put("join", Node.join("join", "join", width).withNext("end"));
        n.put("end", Node.end("end", true, null));
        return new WorkflowDefinition("fan", 1, "fork", n, Set.of(QUEUE));
    }

    private static Map<String, Object> ballast() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("orderId", "A-771");
        m.put("filler", "x".repeat(BALLAST));
        return m;
    }
}
