package com.wiggle.tests;

import com.wiggle.client.WiggleClient;
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
 * Regression suite for {@link FlowSpec#define} against the engine's own behavioural conformance
 * scenarios. {@link Scenarios} pins what the engine does -- gates short-circuit, arms are isolated
 * until a combine, retries are per-policy, a sleep holds no worker. This re-states each of those
 * topologies and runs it to completion on a real server against the conformance suite's own
 * handlers, asserting the outcome that suite asserts.
 *
 * <p>What that catches, and no graph comparison could, is that the names the method references
 * produce are the names the worker binds back. A step named differently would fail to bind before
 * the instance ever ran.
 *
 */
class FlowApiRegressionTest {


    @Test
    @DisplayName("a linear pipeline: the context accumulates in order")
    void sequentialPipeline() throws Exception {
        Scenarios.SeqH h = new Scenarios.SeqH();

        FlowSpec typed = FlowSpec.define("seq", Map.class, SeqSteps.class, (f, s) -> f
                .thenApply(s::one).thenApply(s::two).thenApply(s::three));

        Map<String, Object> out = run(typed, h, Map.of());
        assertEquals(1L, out.get("a"));
        assertEquals(2L, out.get("b"));
        assertEquals(3L, out.get("c"));
    }

    @Test
    @DisplayName("a false gate ends the instance cleanly as gated:gate")
    void gateShortCircuits() throws Exception {
        java.util.concurrent.atomic.AtomicInteger downstream = new java.util.concurrent.atomic.AtomicInteger();
        Scenarios.GatedH h = new Scenarios.GatedH(downstream);

        FlowSpec typed = FlowSpec.define("gated", Map.class, GatedSteps.class, (f, s) -> f
                .thenApply(s::seed).thenFilter(s::gate).thenApply(s::never));

        InstanceView v = runToView(typed, h, Map.of());
        assertEquals("COMPLETED", v.status());
        assertEquals("gated:gate", v.terminationReason(), "a closed gate is a clean end, not an error");
        assertEquals(0, downstream.get(), "nothing downstream of the gate ran");
    }


    @Test
    @DisplayName("fork/combine: both arms' writes survive the join")
    void forkMergesDisjointWrites() throws Exception {
        ForkMerge h = new ForkMerge();

        FlowSpec typed = FlowSpec.define("fork-merge", Map.class, ForkMergeSteps.class, (f, s) -> {
            var seeded = f.thenApply(s::seed);
            var left = seeded.thenApply(s::slowLeft);      // finishes last on purpose
            var right = seeded.thenApply(s::fastRight);
            return Wiggle.allOf(left, right).combineWithContext(s::merge).thenApply(s::after);
        });

        Map<String, Object> out = run(typed, h, Map.of());
        assertEquals("L", out.get("left"), "the slower arm's write survived");
        assertEquals("R", out.get("right"), "and so did the faster one's");
        assertEquals(true, out.get("joined"), "the post-join step ran");
    }

    @Test
    @DisplayName("a three-armed fork runs the continuation exactly once")
    void joinRunsContinuationOnce() throws Exception {
        java.util.concurrent.atomic.AtomicInteger after = new java.util.concurrent.atomic.AtomicInteger();
        JoinOnce h = new JoinOnce(after);

        FlowSpec typed = FlowSpec.define("join-once", Map.class, JoinOnceSteps.class, (f, s) ->
                Wiggle.allOf(f.thenApply(s::a1), f.thenApply(s::b1), f.thenApply(s::c1))
                        .combineWithContext(s::merge)
                        .thenApply(s::after));

        assertEquals("COMPLETED", runToView(typed, h, Map.of()).status());
        Thread.sleep(300);   // give any duplicate dispatch time to show up
        assertEquals(1, after.get(), "the join fires its continuation once, not once per arm");
    }

    @Test
    @DisplayName("nested forks: the inner join pops back to the right barrier")
    void nestedForks() throws Exception {
        Nested h = new Nested();

        // NOTE the method names. An arm is named after its last step, and the engine stages each arm's
        // result in the context under that name -- so an arm name shares the key namespace with the
        // context. A handler named after a key its own arm writes would collide; these are named for
        // their position and write their keys, which is the habit to keep.
        FlowSpec typed = FlowSpec.define("nested", Map.class, NestedSteps.class, (f, s) -> {
            var left = Wiggle.allOf(f.thenApply(s::innerA), f.thenApply(s::innerB))
                    .combineWithContext(s::innerMerge)
                    .thenApply(s::innerDone);
            var right = f.thenApply(s::outerRight);
            return Wiggle.allOf(left, right).combineWithContext(s::outerMerge).thenApply(s::outerDone);
        });

        Map<String, Object> out = run(typed, h, Map.of());
        assertEquals(1L, out.get("ia"));
        assertEquals(1L, out.get("ib"));
        assertEquals(1L, out.get("innerAfter"), "the inner join resumed its own branch");
        assertEquals(1L, out.get("or"));
        assertEquals(1L, out.get("outerAfter"), "and the outer join resumed the trunk");
    }

    @Test
    @DisplayName("a gate inside an arm: siblings join without it")
    void gateInsideBranchDoesNotStrandSiblings() throws Exception {
        BranchGate h = new BranchGate();

        FlowSpec typed = FlowSpec.define("branch-gate", Map.class, BranchGateSteps.class, (f, s) -> {
            var gated = f.thenFilter(s::gate).thenApply(s::skipped);
            var other = f.thenApply(s::ran);
            return Wiggle.allOf(gated, other).combineWithContext(s::merge).thenApply(s::after);
        });

        Map<String, Object> out = run(typed, h, Map.of());
        assertNull(out.get("skipped"), "the closed arm contributed nothing");
        assertEquals(true, out.get("ran"), "its sibling still ran");
        assertEquals(true, out.get("after"), "and the join was not stranded");
    }


    @Test
    @DisplayName("a per-step retry policy retries a transient failure")
    void retriesTransientFailures() throws Exception {
        Map<String, java.util.concurrent.atomic.AtomicInteger> attempts = new java.util.concurrent.ConcurrentHashMap<>();
        Scenarios.RetryH h = new Scenarios.RetryH(attempts);
        RetryPolicy policy = RetryPolicy.fixed(5, Duration.ofMillis(50));

        FlowSpec typed = FlowSpec.define("retry", Map.class, RetrySteps.class,
                (f, s) -> f.thenApply(s::flaky, policy));

        Map<String, Object> out = run(typed, h, Map.of());
        assertEquals(3L, out.get("attempts"), "it failed twice and succeeded on the third attempt");
    }

    @Test
    @DisplayName("a server-side timer defers the instance without holding a worker")
    void sleepDefersWithoutHoldingAWorker() throws Exception {
        Scenarios.SleeperH h = new Scenarios.SleeperH();

        FlowSpec typed = FlowSpec.define("sleeper", Map.class, SleeperSteps.class, (f, s) -> f
                .thenApply(s::before)
                .thenSleep("nap", Duration.ofMillis(400))
                .thenApply(s::after));

        Map<String, Object> out = run(typed, h, Map.of());
        long before = ((Number) out.get("before")).longValue();
        long after = ((Number) out.get("after")).longValue();
        assertTrue(after - before >= 350, "the timer actually deferred: " + (after - before) + "ms");
    }


    @Test
    @DisplayName("a typed definition is content-addressed the same way, and rejects the same graphs")
    void definitionIdentityIsUnchanged() {
        Scenarios.SeqH h = new Scenarios.SeqH();

        FlowSpec a = FlowSpec.define("versioned", Map.class, SeqSteps.class, (f, s) -> f.thenApply(s::one));
        FlowSpec b = FlowSpec.define("versioned", Map.class, SeqSteps.class, (f, s) -> f.thenApply(s::one));
        FlowSpec c = FlowSpec.define("versioned", Map.class, SeqSteps.class,
                (f, s) -> f.thenApply(s::one).thenApply(s::two));

        assertEquals(a.version(), b.version(), "the same topology twice is the same version");
        assertTrue(a.version() != c.version(), "a different topology is a different version");

        // and the graph-level rules still bite: a duplicate node name is still rejected
        assertTrue(org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                        () -> FlowSpec.define("dup", Map.class, SeqSteps.class,
                                (f, s) -> f.thenApply(s::one).thenApply(s::one)))
                .getMessage().contains("duplicate step name"));
    }

    //
    // What each spec names. The handlers below implement them, so the compiler checks that every step
    // the spec declares exists with the right signature -- and none of these interfaces has, or needs,
    // an implementation at definition time.

    interface SeqSteps {
        Map<String, Object> one(Map<String, Object> c);
        Map<String, Object> two(Map<String, Object> c);
        Map<String, Object> three(Map<String, Object> c);
    }

    interface GatedSteps {
        Map<String, Object> seed(Map<String, Object> c);
        boolean gate(Map<String, Object> c);
        Map<String, Object> never(Map<String, Object> c);
    }

    interface ForkMergeSteps {
        Map<String, Object> seed(Map<String, Object> c);
        Map<String, Object> slowLeft(Map<String, Object> c);
        Map<String, Object> fastRight(Map<String, Object> c);
        Map<String, Object> merge(@com.wiggle.client.worker.Context Map<String, Object> base,
                                  Map<String, Object> left, Map<String, Object> right);
        Map<String, Object> after(Map<String, Object> c);
    }

    interface JoinOnceSteps {
        Map<String, Object> a1(Map<String, Object> c);
        Map<String, Object> b1(Map<String, Object> c);
        Map<String, Object> c1(Map<String, Object> c);
        Map<String, Object> merge(@com.wiggle.client.worker.Context Map<String, Object> base,
                                  Map<String, Object> a, Map<String, Object> b, Map<String, Object> c);
        Map<String, Object> after(Map<String, Object> c);
    }

    interface NestedSteps {
        Map<String, Object> innerA(Map<String, Object> c);
        Map<String, Object> innerB(Map<String, Object> c);
        Map<String, Object> innerMerge(@com.wiggle.client.worker.Context Map<String, Object> base,
                                       Map<String, Object> ia, Map<String, Object> ib);
        Map<String, Object> innerDone(Map<String, Object> c);
        Map<String, Object> outerRight(Map<String, Object> c);
        Map<String, Object> outerMerge(@com.wiggle.client.worker.Context Map<String, Object> base,
                                       Map<String, Object> left, Map<String, Object> right);
        Map<String, Object> outerDone(Map<String, Object> c);
    }

    interface BranchGateSteps {
        boolean gate(Map<String, Object> c);
        Map<String, Object> skipped(Map<String, Object> c);
        Map<String, Object> ran(Map<String, Object> c);
        Map<String, Object> merge(@com.wiggle.client.worker.Context Map<String, Object> base,
                                  Map<String, Object> gated, Map<String, Object> other);
        Map<String, Object> after(Map<String, Object> c);
    }

    interface RetrySteps {
        Map<String, Object> flaky(Map<String, Object> c);
    }

    interface SleeperSteps {
        Map<String, Object> before(Map<String, Object> c);
        Map<String, Object> after(Map<String, Object> c);
    }

    //
    // Same logic as the conformance suite's. A combine takes one parameter per arm, in fork order.

    @com.wiggle.client.worker.ForFlow("fork-merge")
    public static final class ForkMerge implements ForkMergeSteps {
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

    @com.wiggle.client.worker.ForFlow("join-once")
    public static final class JoinOnce implements JoinOnceSteps {
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

    @com.wiggle.client.worker.ForFlow("nested")
    public static final class Nested implements NestedSteps {
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

    @com.wiggle.client.worker.ForFlow("branch-gate")
    public static final class BranchGate implements BranchGateSteps {
        public boolean gate(Map<String, Object> c) { return false; }
        public Map<String, Object> skipped(Map<String, Object> c) { return Scenarios.put(c, "skipped", true); }
        public Map<String, Object> ran(Map<String, Object> c) { return Scenarios.put(c, "ran", true); }
        public Map<String, Object> merge(@com.wiggle.client.worker.Context Map<String, Object> base,
                                         Map<String, Object> gated, Map<String, Object> other) {
            return Scenarios.fold(base, gated, other);
        }
        public Map<String, Object> after(Map<String, Object> c) { return Scenarios.put(c, "after", true); }
    }


    /** Equal content hashes: the two definitions are the same graph, so the engine cannot tell them apart. */

    private static Map<String, Object> run(FlowSpec spec, Object handlers, Map<String, Object> input)
            throws Exception {
        InstanceView v = runToView(spec, handlers, input);
        assertEquals("COMPLETED", v.status(), "status (error: " + v.error() + ")");
        return Json.asObject(v.context());
    }

    /**
     * Runs one instance to completion on a one-node server.
     *
     * <p>Storage comes from {@link TestStorage}: H2 in PostgreSQL mode by default, or a real
     * PostgreSQL when one is configured -- the same scenarios, over the real dialect and its
     * {@code FOR UPDATE SKIP LOCKED} claim.
     */
    private static InstanceView runToView(FlowSpec spec, Object handlers, Map<String, Object> input)
            throws Exception {
        com.wiggle.server.ServerConfig config = new com.wiggle.server.ServerConfig(
                0, "node-0", TestStorage.url("flowreg"), TestStorage.user(), TestStorage.password(), 8,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0, Duration.ofSeconds(5), Duration.ofSeconds(10));
        try (com.wiggle.server.WiggleServer server =
                     new com.wiggle.server.WiggleServer(config, new com.wiggle.dist.WiggleStorageFactory()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            Worker w = new Worker(client, "w-" + System.nanoTime(),
                    WorkerOptions.defaults().withConcurrency(4).withLongPollWait(Duration.ofMillis(250)));
            client.register(spec);
            w.registerHandler(handlers);
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
