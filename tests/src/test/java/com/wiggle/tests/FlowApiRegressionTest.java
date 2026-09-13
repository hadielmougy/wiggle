package com.wiggle.tests;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.Branch;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.flow.Wiggle;
import com.wiggle.client.worker.Worker;
import com.wiggle.client.worker.WorkerOptions;
import com.wiggle.core.InstanceView;
import com.wiggle.core.Json;
import com.wiggle.core.RetryPolicy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression suite for the typed API ({@link Wiggle#define}) against the engine's own behavioural
 * conformance scenarios. {@link Scenarios} pins what the engine does -- gates short-circuit, arms are
 * isolated until a combine, retries are per-policy, a sleep holds no worker -- with every topology
 * written through {@link Wiggle#graph}. This re-states each of those topologies through the typed API
 * and checks two things:
 *
 * <ol>
 *   <li><b>the same graph</b> -- content hashes must match, node for node. A definition's version is
 *       a SHA-256 over the whole canonical topology, so equal hashes mean the engine is driving a
 *       byte-identical graph and <em>cannot</em> behave differently;</li>
 *   <li><b>the same behaviour</b> -- each one then runs to completion on a real server against the
 *       conformance suite's own handlers, asserting the outcome that suite asserts.</li>
 * </ol>
 *
 * The second is what the first cannot prove: that the names the method references produced are the
 * names the worker binds back. If the typed API named a step differently, binding would fail before
 * the instance ever ran.
 *
 * <p>One difference is expected and is visible below. A typed step is named after its <em>method</em>
 * ({@code slowLeft}), where the conformance suite writes its graphs in kebab-case ({@code slow-left}).
 * Step binding folds the two together, so behaviour is unaffected -- but a fork arm's name is an exact
 * map key, so the fan-out cases compare against graphs named the way the method references name them,
 * and their combines bind {@linkplain com.wiggle.client.worker.Arm by position} rather than by an
 * {@code @Arm} that would have to spell the derived name out.
 */
class FlowApiRegressionTest {

    // ------------------------------------------------------------------ linear: steps and gates

    @Test
    @DisplayName("a linear pipeline: same graph, and the context still accumulates in order")
    void sequentialPipeline() throws Exception {
        Scenarios.SeqH h = new Scenarios.SeqH();

        FlowSpec graph = Wiggle.graph("seq").step("one").step("two").step("three").build();
        FlowSpec typed = Wiggle.define("seq", Map.class, f -> f
                .thenApply(h::one).thenApply(h::two).thenApply(h::three));

        assertSameGraph(graph, typed);

        Map<String, Object> out = run(typed, h, Map.of());
        assertEquals(1L, out.get("a"));
        assertEquals(2L, out.get("b"));
        assertEquals(3L, out.get("c"));
    }

    @Test
    @DisplayName("a false gate: same graph, and it still ends the instance cleanly as gated:gate")
    void gateShortCircuits() throws Exception {
        java.util.concurrent.atomic.AtomicInteger downstream = new java.util.concurrent.atomic.AtomicInteger();
        Scenarios.GatedH h = new Scenarios.GatedH(downstream);

        FlowSpec graph = Wiggle.graph("gated").step("seed").gate("gate").step("never").build();
        FlowSpec typed = Wiggle.define("gated", Map.class, f -> f
                .thenApply(h::seed).thenFilter(h::gate).thenApply(h::never));

        assertSameGraph(graph, typed);

        InstanceView v = runToView(typed, h, Map.of());
        assertEquals("COMPLETED", v.status());
        assertEquals("gated:gate", v.terminationReason(), "a closed gate is a clean end, not an error");
        assertEquals(0, downstream.get(), "nothing downstream of the gate ran");
    }

    // ------------------------------------------------------------------ fan-out and combine

    @Test
    @DisplayName("fork/combine: same graph, and both arms' writes still survive the join")
    void forkMergesDisjointWrites() throws Exception {
        ForkMerge h = new ForkMerge();

        FlowSpec graph = Wiggle.graph("fork-merge")
                .step("seed")
                .fork(Branch.of("slowLeft", s -> s.step("slowLeft")),
                      Branch.of("fastRight", s -> s.step("fastRight")))
                .combine("merge")
                .step("after")
                .build();

        FlowSpec typed = Wiggle.define("fork-merge", Map.class, f -> {
            var seeded = f.thenApply(h::seed);
            var left = seeded.thenApply(h::slowLeft);      // finishes last on purpose
            var right = seeded.thenApply(h::fastRight);
            return Wiggle.allOf(left, right).combineWithContext(h::merge).thenApply(h::after);
        });

        assertSameGraph(graph, typed);

        Map<String, Object> out = run(typed, h, Map.of());
        assertEquals("L", out.get("left"), "the slower arm's write survived");
        assertEquals("R", out.get("right"), "and so did the faster one's");
        assertEquals(true, out.get("joined"), "the post-join step ran");
    }

    @Test
    @DisplayName("a three-armed fork: same graph, and the continuation still runs exactly once")
    void joinRunsContinuationOnce() throws Exception {
        java.util.concurrent.atomic.AtomicInteger after = new java.util.concurrent.atomic.AtomicInteger();
        JoinOnce h = new JoinOnce(after);

        FlowSpec graph = Wiggle.graph("join-once")
                .fork(Branch.of("a1", s -> s.step("a1")),
                      Branch.of("b1", s -> s.step("b1")),
                      Branch.of("c1", s -> s.step("c1")))
                .combine("merge")
                .step("after")
                .build();

        FlowSpec typed = Wiggle.define("join-once", Map.class, f ->
                Wiggle.allOf(f.thenApply(h::a1), f.thenApply(h::b1), f.thenApply(h::c1))
                        .combineWithContext(h::merge)
                        .thenApply(h::after));

        assertSameGraph(graph, typed);

        assertEquals("COMPLETED", runToView(typed, h, Map.of()).status());
        Thread.sleep(300);   // give any duplicate dispatch time to show up
        assertEquals(1, after.get(), "the join fires its continuation once, not once per arm");
    }

    @Test
    @DisplayName("nested forks: same graph, and the inner join still pops back to the right barrier")
    void nestedForks() throws Exception {
        Nested h = new Nested();

        // NOTE the method names. An arm is named after its last step, and the engine stages each arm's
        // result in the context under that name -- so an arm name shares the key namespace with the
        // context. A handler named after a key its own arm writes would collide; these are named for
        // their position and write their keys, which is the habit to keep.
        FlowSpec graph = Wiggle.graph("nested")
                .fork(Branch.of("innerDone", s -> s
                              .fork(Branch.of("innerA", t -> t.step("innerA")),
                                    Branch.of("innerB", t -> t.step("innerB")))
                              .combine("innerMerge")
                              .step("innerDone")),
                      Branch.of("outerRight", s -> s.step("outerRight")))
                .combine("outerMerge")
                .step("outerDone")
                .build();

        FlowSpec typed = Wiggle.define("nested", Map.class, f -> {
            var left = Wiggle.allOf(f.thenApply(h::innerA), f.thenApply(h::innerB))
                    .combineWithContext(h::innerMerge)
                    .thenApply(h::innerDone);
            var right = f.thenApply(h::outerRight);
            return Wiggle.allOf(left, right).combineWithContext(h::outerMerge).thenApply(h::outerDone);
        });

        assertSameGraph(graph, typed);

        Map<String, Object> out = run(typed, h, Map.of());
        assertEquals(1L, out.get("ia"));
        assertEquals(1L, out.get("ib"));
        assertEquals(1L, out.get("innerAfter"), "the inner join resumed its own branch");
        assertEquals(1L, out.get("or"));
        assertEquals(1L, out.get("outerAfter"), "and the outer join resumed the trunk");
    }

    @Test
    @DisplayName("a gate inside an arm: same graph, and siblings still join without it")
    void gateInsideBranchDoesNotStrandSiblings() throws Exception {
        BranchGate h = new BranchGate();

        FlowSpec graph = Wiggle.graph("branch-gate")
                .fork(Branch.of("skipped", s -> s.gate("gate").step("skipped")),
                      Branch.of("ran", s -> s.step("ran")))
                .combine("merge")
                .step("after")
                .build();

        FlowSpec typed = Wiggle.define("branch-gate", Map.class, f -> {
            var gated = f.thenFilter(h::gate).thenApply(h::skipped);
            var other = f.thenApply(h::ran);
            return Wiggle.allOf(gated, other).combineWithContext(h::merge).thenApply(h::after);
        });

        assertSameGraph(graph, typed);

        Map<String, Object> out = run(typed, h, Map.of());
        assertNull(out.get("skipped"), "the closed arm contributed nothing");
        assertEquals(true, out.get("ran"), "its sibling still ran");
        assertEquals(true, out.get("after"), "and the join was not stranded");
    }

    // ------------------------------------------------------------------ failure handling

    @Test
    @DisplayName("a per-step retry policy: same graph, and a transient failure is still retried")
    void retriesTransientFailures() throws Exception {
        Map<String, java.util.concurrent.atomic.AtomicInteger> attempts = new java.util.concurrent.ConcurrentHashMap<>();
        Scenarios.RetryH h = new Scenarios.RetryH(attempts);
        RetryPolicy policy = RetryPolicy.fixed(5, Duration.ofMillis(50));

        FlowSpec graph = Wiggle.graph("retry").step("flaky", policy).build();
        FlowSpec typed = Wiggle.define("retry", Map.class, f -> f.thenApply(h::flaky, policy));

        assertSameGraph(graph, typed);

        Map<String, Object> out = run(typed, h, Map.of());
        assertEquals(3L, out.get("attempts"), "it failed twice and succeeded on the third attempt");
    }

    @Test
    @DisplayName("a server-side timer: same graph, and the instance still defers without a worker")
    void sleepDefersWithoutHoldingAWorker() throws Exception {
        Scenarios.SleeperH h = new Scenarios.SleeperH();

        FlowSpec graph = Wiggle.graph("sleeper")
                .step("before")
                .sleep("nap", Duration.ofMillis(400))
                .step("after")
                .build();

        FlowSpec typed = Wiggle.define("sleeper", Map.class, f -> f
                .thenApply(h::before)
                .thenSleep("nap", Duration.ofMillis(400))
                .thenApply(h::after));

        assertSameGraph(graph, typed);

        Map<String, Object> out = run(typed, h, Map.of());
        long before = ((Number) out.get("before")).longValue();
        long after = ((Number) out.get("after")).longValue();
        assertTrue(after - before >= 350, "the timer actually deferred: " + (after - before) + "ms");
    }

    // ------------------------------------------------------------------ the graph itself

    @Test
    @DisplayName("a typed definition is content-addressed the same way, and rejects the same graphs")
    void definitionIdentityIsUnchanged() {
        Scenarios.SeqH h = new Scenarios.SeqH();

        FlowSpec a = Wiggle.define("versioned", Map.class, f -> f.thenApply(h::one));
        FlowSpec b = Wiggle.define("versioned", Map.class, f -> f.thenApply(h::one));
        FlowSpec c = Wiggle.define("versioned", Map.class, f -> f.thenApply(h::one).thenApply(h::two));

        assertEquals(a.version(), b.version(), "the same topology twice is the same version");
        assertTrue(a.version() != c.version(), "a different topology is a different version");

        // and the graph-level rules still bite: a duplicate node name is still rejected
        assertTrue(org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                        () -> Wiggle.define("dup", Map.class, f -> f.thenApply(h::one).thenApply(h::one)))
                .getMessage().contains("duplicate step name"));
    }

    // ------------------------------------------------------------------ fan-out handlers
    //
    // Same logic as the conformance suite's, with the combines binding by position instead of by
    // @Arm -- which is what a fan-out defined through method references wants, since it never has to
    // spell an arm name out.

    @com.wiggle.client.worker.Handlers("fork-merge")
    public static final class ForkMerge {
        public Map<String, Object> seed(Map<String, Object> c) { return Scenarios.put(c, "seeded", true); }
        public Map<String, Object> slowLeft(Map<String, Object> c) {
            // finishes last on purpose; a FlowFn declares no checked exception, so absorb it here
            try { Thread.sleep(150); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            return Scenarios.put(c, "left", "L");
        }
        public Map<String, Object> fastRight(Map<String, Object> c) { return Scenarios.put(c, "right", "R"); }
        public Map<String, Object> merge(@com.wiggle.client.worker.Context Map<String, Object> base,
                                         Map<String, Object> left, Map<String, Object> right) {
            return Scenarios.fold(base, left, right);
        }
        public Map<String, Object> after(Map<String, Object> c) { return Scenarios.put(c, "joined", true); }
    }

    @com.wiggle.client.worker.Handlers("join-once")
    public static final class JoinOnce {
        private final java.util.concurrent.atomic.AtomicInteger after;
        JoinOnce(java.util.concurrent.atomic.AtomicInteger after) { this.after = after; }
        public Map<String, Object> a1(Map<String, Object> c) { return Scenarios.put(c, "a", 1L); }
        public Map<String, Object> b1(Map<String, Object> c) { return Scenarios.put(c, "b", 1L); }
        public Map<String, Object> c1(Map<String, Object> c) { return Scenarios.put(c, "c", 1L); }
        public Map<String, Object> merge(@com.wiggle.client.worker.Context Map<String, Object> base,
                                         Map<String, Object> a, Map<String, Object> b, Map<String, Object> c) {
            return Scenarios.fold(base, a, b, c);
        }
        public Map<String, Object> after(Map<String, Object> c) {
            after.incrementAndGet();
            return Scenarios.put(c, "after", 1L);
        }
    }

    @com.wiggle.client.worker.Handlers("nested")
    public static final class Nested {
        public Map<String, Object> innerA(Map<String, Object> c) { return Scenarios.put(c, "ia", 1L); }
        public Map<String, Object> innerB(Map<String, Object> c) { return Scenarios.put(c, "ib", 1L); }
        public Map<String, Object> innerDone(Map<String, Object> c) { return Scenarios.put(c, "innerAfter", 1L); }
        public Map<String, Object> outerRight(Map<String, Object> c) { return Scenarios.put(c, "or", 1L); }
        public Map<String, Object> innerMerge(@com.wiggle.client.worker.Context Map<String, Object> base,
                                              Map<String, Object> ia, Map<String, Object> ib) {
            return Scenarios.fold(base, ia, ib);
        }
        public Map<String, Object> outerMerge(@com.wiggle.client.worker.Context Map<String, Object> base,
                                              Map<String, Object> left, Map<String, Object> right) {
            return Scenarios.fold(base, left, right);
        }
        public Map<String, Object> outerDone(Map<String, Object> c) { return Scenarios.put(c, "outerAfter", 1L); }
    }

    @com.wiggle.client.worker.Handlers("branch-gate")
    public static final class BranchGate {
        public boolean gate(Map<String, Object> c) { return false; }
        public Map<String, Object> skipped(Map<String, Object> c) { return Scenarios.put(c, "skipped", true); }
        public Map<String, Object> ran(Map<String, Object> c) { return Scenarios.put(c, "ran", true); }
        public Map<String, Object> merge(@com.wiggle.client.worker.Context Map<String, Object> base,
                                         Map<String, Object> gated, Map<String, Object> other) {
            return Scenarios.fold(base, gated, other);
        }
        public Map<String, Object> after(Map<String, Object> c) { return Scenarios.put(c, "after", true); }
    }

    // ------------------------------------------------------------------ harness

    /** Equal content hashes: the two definitions are the same graph, so the engine cannot tell them apart. */
    private static void assertSameGraph(FlowSpec graph, FlowSpec typed) {
        assertEquals(graph.definition().nodes().keySet(), typed.definition().nodes().keySet(), "node ids");
        assertEquals(graph.version(), typed.version(),
                "Wiggle.define must compile to the same graph as Wiggle.graph, node for node");
    }

    private static Map<String, Object> run(FlowSpec spec, Object handlers, Map<String, Object> input)
            throws Exception {
        InstanceView v = runToView(spec, handlers, input);
        assertEquals("COMPLETED", v.status(), "status (error: " + v.error() + ")");
        return Json.asObject(v.context());
    }

    /** Runs one instance to completion on a one-node in-memory H2 server. */
    private static InstanceView runToView(FlowSpec spec, Object handlers, Map<String, Object> input)
            throws Exception {
        String url = "jdbc:h2:mem:flowreg-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
        com.wiggle.server.ServerConfig config = new com.wiggle.server.ServerConfig(
                0, "node-0", url, "sa", "", 8,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0, Duration.ofSeconds(5), Duration.ofSeconds(10));
        try (com.wiggle.server.WiggleServer server =
                     new com.wiggle.server.WiggleServer(config, new com.wiggle.dist.WiggleStorageFactory()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            Worker w = new Worker(client, "w-0",
                    WorkerOptions.defaults().withConcurrency(4).withLongPollWait(Duration.ofMillis(250)));
            w.register(spec).handlers(handlers);
            w.start();
            try {
                InstanceView v = client.awaitCompletion(client.start(spec, input), Duration.ofSeconds(30));
                assertNotNull(v, "instance view");
                return v;
            } finally {
                w.close();
            }
        }
    }
}