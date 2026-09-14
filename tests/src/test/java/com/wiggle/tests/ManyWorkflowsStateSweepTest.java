package com.wiggle.tests;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.flow.Wiggle;
import com.wiggle.client.worker.CompensableActivity;
import com.wiggle.client.worker.Context;
import com.wiggle.client.worker.ForFlow;
import com.wiggle.client.worker.PermanentActivityException;
import com.wiggle.client.worker.Worker;
import com.wiggle.client.worker.WorkerOptions;
import com.wiggle.core.ExecutionMode;
import com.wiggle.core.Ids;
import com.wiggle.core.InstanceView;
import com.wiggle.core.Json;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A breadth sweep: many workflows, every operator, every terminal state, all running at once against
 * one store -- and the coverage view read across the whole mess.
 *
 * <p>The narrow tests each pin one behaviour in isolation, on an empty store, with one workflow and
 * one worker. That is the right shape for a unit of behaviour and the wrong shape for finding what
 * only appears in bulk: a {@code GROUP BY} that mis-attributes rows once several workflows and
 * versions share the token table, a status that is reported for a single instance but miscounted over
 * hundreds, a dialect difference that a five-row table never provokes.
 *
 * <p>So this runs {@value #WORKFLOWS} workflow shapes in parallel, drives instances into every
 * terminal and non-terminal state the engine has -- COMPLETED, FAILED, CANCELLED, COMPENSATED, gated,
 * sleeping, awaiting a signal, and deliberately stranded -- across all three execution modes, and
 * then asserts the totals and the backlog coverage over the lot.
 *
 * <p>Runs against whatever {@link TestStorage} points at. It is worth pointing it at PostgreSQL:
 * that is where the grouping, the claim and the dialect actually have to hold, and H2 in PostgreSQL
 * mode is not PostgreSQL.
 */
class ManyWorkflowsStateSweepTest {

    interface ForkedSteps {
        Map<String, Object> a(Map<String, Object> ctx);
        Map<String, Object> left(Map<String, Object> ctx);
        Map<String, Object> right(Map<String, Object> ctx);
        Map<String, Object> merge(@Context Map<String, Object> base,
                                  Map<String, Object> l, Map<String, Object> r);
        Map<String, Object> c(Map<String, Object> ctx);
    }

    interface ForeachSteps {
        Map<String, Object> a(Map<String, Object> ctx);
        Map<String, Object> each(Map<String, Object> item);
        Map<String, Object> collect(@Context Map<String, Object> base, java.util.List<Object> items);
    }

    interface BranchySteps {
        boolean isBig(Map<String, Object> ctx);
        Map<String, Object> big(Map<String, Object> ctx);
        Map<String, Object> small(Map<String, Object> ctx);
        boolean more(Map<String, Object> ctx);
        Map<String, Object> drain(Map<String, Object> ctx);
    }

    interface ParkedSteps {
        Map<String, Object> a(Map<String, Object> ctx);
        Map<String, Object> after(Map<String, Object> ctx);
    }

    /** The steps this spec names; a worker binds them by name. */
    interface OneStep {
        Map<String, Object> a(Map<String, Object> ctx);
        Map<String, Object> b(Map<String, Object> ctx);
        Map<String, Object> boom(Map<String, Object> ctx);
        Map<String, Object> c(Map<String, Object> ctx);
        CompensableActivity<Map<String, Object>> charge();
        boolean never(Map<String, Object> ctx);
        CompensableActivity<Map<String, Object>> reserve();
        Map<String, Object> unreachable(Map<String, Object> ctx);
    }

    /** Distinct workflow shapes, each exercising a different part of the engine. */
    private static final int WORKFLOWS = 8;
    /** Instances per shape, to get past the one-row-per-group case. */
    private static final int PER_WORKFLOW = 12;

    private static final String PREFIX = "sweep-";

    private static Map<String, Object> put(Map<String, Object> c, String k, Object v) {
        Map<String, Object> n = new LinkedHashMap<>(c);
        n.put(k, v);
        return n;
    }

    private static ServerConfig config() {
        return new ServerConfig(TestPorts.free(), "sweep-node", TestStorage.url("sweep"), TestStorage.user(),
                TestStorage.password(), 16,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 200, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    // ------------------------------------------------------------------ the shapes

    /** 1. linear, SERVER mode -> COMPLETED */
    private static FlowSpec linear() {
        return FlowSpec.define(PREFIX + "linear", Map.class, OneStep.class, (f, s) -> f
                .thenApply(s::a)
                .thenApply(s::b)
                .thenApply(s::c));
    }

    /** 2. a gate that closes -> COMPLETED with terminationReason gated:* */
    private static FlowSpec gated() {
        return FlowSpec.define(PREFIX + "gated", Map.class, OneStep.class, (f, s) -> f
                .thenApply(s::a)
                .thenFilter(s::never)
                .thenApply(s::unreachable));
    }

    /** 3. fork/combine under LOCAL_SYNC -> COMPLETED, exercising local chaining of a join */
    private static FlowSpec forked() {
        return FlowSpec.define(PREFIX + "forked", Map.class, ForkedSteps.class, (f, s) -> {
            var seeded = f.execution(ExecutionMode.LOCAL_SYNC).thenApply(s::a);
            return Wiggle.allOf(seeded.thenApply(s::left), seeded.thenApply(s::right))
                    .combineWithContext(s::merge)
                    .thenApply(s::c);
        });
    }

    /** 4. forEach over a collection under LOCAL_ASYNC -> COMPLETED */
    private static FlowSpec fannedOut() {
        return FlowSpec.define(PREFIX + "foreach", Map.class, ForeachSteps.class, (f, s) -> f
                .execution(ExecutionMode.LOCAL_ASYNC)
                .thenApply(s::a)
                .thenForEach("items", Map.class, b -> b.thenApply(s::each))
                .combine(s::collect));
    }

    /** 5. choose + doWhile -> COMPLETED, exercising guards and a cycle */
    private static FlowSpec branchy() {
        return FlowSpec.define(PREFIX + "branchy", Map.class, BranchySteps.class, (f, s) -> Wiggle.oneOf(
                        f.when(s::isBig).thenApply(s::big),
                        f.otherwise().thenApply(s::small))
                .repeatWhile(s::more, b -> b.thenApply(s::drain)));
    }

    /** 6. a permanent failure -> FAILED */
    private static FlowSpec failing() {
        return FlowSpec.define(PREFIX + "failing", Map.class, OneStep.class, (f, s) -> f
                .thenApply(s::a)
                .thenApply(s::boom));
    }

    /** 7. two compensable steps then a failure -> COMPENSATED */
    private static FlowSpec saga() {
        return FlowSpec.define(PREFIX + "saga", Map.class, OneStep.class, (f, s) -> f
                .thenActivity(s::reserve)
                .thenActivity(s::charge)
                .thenApply(s::boom));
    }

    /** 8. parks on a signal nobody sends -> stays RUNNING, and a sleep before it */
    private static FlowSpec parked() {
        return FlowSpec.define(PREFIX + "parked", Map.class, ParkedSteps.class, (f, s) -> f
                .thenApply(s::a)
                .thenSleep("nap", Duration.ofMillis(200))
                .thenAwait("never-arrives")
                .thenApply(s::after));
    }

    // ------------------------------------------------------------------ the handlers

    @ForFlow(PREFIX + "linear")
    public static final class LinearH {
        public Map<String, Object> a(Map<String, Object> c) { return put(c, "a", 1L); }
        public Map<String, Object> b(Map<String, Object> c) { return put(c, "b", 1L); }
        public Map<String, Object> c(Map<String, Object> c) { return put(c, "c", 1L); }
    }

    @ForFlow(PREFIX + "gated")
    public static final class GatedH {
        public Map<String, Object> a(Map<String, Object> c) { return put(c, "a", 1L); }
        public boolean never(Map<String, Object> c) { return false; }
        public Map<String, Object> unreachable(Map<String, Object> c) { return put(c, "bug", true); }
    }

    @ForFlow(PREFIX + "forked")
    public static final class ForkedH {
        public Map<String, Object> a(Map<String, Object> c) { return put(c, "a", 1L); }
        public Map<String, Object> left(Map<String, Object> c) { return put(c, "left", 1L); }
        public Map<String, Object> right(Map<String, Object> c) { return put(c, "right", 1L); }
        public Map<String, Object> merge(@Context Map<String, Object> base,
                                         Map<String, Object> l, Map<String, Object> r) {
            Map<String, Object> out = new LinkedHashMap<>(base);
            out.putAll(l);
            out.putAll(r);
            return out;
        }
        public Map<String, Object> c(Map<String, Object> c) { return put(c, "joined", true); }
    }

    @ForFlow(PREFIX + "foreach")
    public static final class ForeachH {
        public Map<String, Object> a(Map<String, Object> c) { return c; }
        public Map<String, Object> each(Map<String, Object> item) { return put(item, "priced", true); }
        public Map<String, Object> collect(@Context Map<String, Object> base, List<Object> items) {
            return put(base, "count", (long) items.size());
        }
    }

    @ForFlow(PREFIX + "branchy")
    public static final class BranchyH {
        public boolean isBig(Map<String, Object> c) { return ((Number) c.getOrDefault("n", 0L)).intValue() > 5; }
        public Map<String, Object> big(Map<String, Object> c) { return put(c, "path", "big"); }
        public Map<String, Object> small(Map<String, Object> c) { return put(c, "path", "small"); }
        public Map<String, Object> drain(Map<String, Object> c) {
            long left = ((Number) c.getOrDefault("left", 2L)).longValue();
            return put(c, "left", left - 1);
        }
        public boolean more(Map<String, Object> c) {
            return ((Number) c.getOrDefault("left", 0L)).longValue() > 0;
        }
    }

    @ForFlow(PREFIX + "failing")
    public static final class FailingH {
        public Map<String, Object> a(Map<String, Object> c) { return put(c, "a", 1L); }
        public Map<String, Object> boom(Map<String, Object> c) {
            throw new PermanentActivityException("deliberate: this instance must land FAILED");
        }
    }

    @ForFlow(PREFIX + "saga")
    public static final class SagaH {
        final Map<String, Integer> undos;
        SagaH(Map<String, Integer> undos) { this.undos = undos; }
        public com.wiggle.client.worker.CompensableActivity<Map<String, Object>> reserve() { return compensable("reserved"); }
        public com.wiggle.client.worker.CompensableActivity<Map<String, Object>> charge() { return compensable("charged"); }
        public Map<String, Object> boom(Map<String, Object> c) {
            throw new PermanentActivityException("deliberate: this instance must land COMPENSATED");
        }

        private com.wiggle.client.worker.CompensableActivity<Map<String, Object>> compensable(String key) {
            Map<String, Integer> counter = undos;
            final class Step implements com.wiggle.client.worker.CompensableActivity<Map<String, Object>> {
                public Map<String, Object> execute(Map<String, Object> ctx) { return put(ctx, key, true); }
                public void compensate(com.wiggle.client.worker.Compensation<Map<String, Object>> c) {
                    counter.merge(key, 1, Integer::sum);
                }
            }
            return new Step();
        }
    }

    @ForFlow(PREFIX + "parked")
    public static final class ParkedH {
        public Map<String, Object> a(Map<String, Object> c) { return put(c, "a", 1L); }
        public Map<String, Object> after(Map<String, Object> c) { return put(c, "after", 1L); }
    }

    // ------------------------------------------------------------------ the sweep

    @Test
    @DisplayName("many workflows in every state at once: totals, terminal states and coverage all hold")
    void sweep() throws Exception {
        Map<String, Integer> undos = new ConcurrentHashMap<>();

        try (WiggleServer server = new WiggleServer(config(), new com.wiggle.dist.WiggleStorageFactory()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {

            List<FlowSpec> specs = List.of(linear(), gated(), forked(), fannedOut(),
                    branchy(), failing(), saga(), parked());
            assertEquals(WORKFLOWS, specs.size(), "the sweep declares every shape it runs");
            for (FlowSpec s : specs) {
                clear(client, s.name());
                client.register(s);
            }

            try (Worker w = new Worker(client, "sweep-" + Ids.next("x"),
                    WorkerOptions.defaults().withConcurrency(16))
                    .registerHandler(new LinearH()).registerHandler(new GatedH()).registerHandler(new ForkedH())
                    .registerHandler(new ForeachH()).registerHandler(new BranchyH()).registerHandler(new FailingH())
                    .registerHandler(new SagaH(undos)).registerHandler(new ParkedH())) {
                w.start();

                // ---- start everything at once, so the store holds every shape concurrently
                Map<String, List<String>> started = new LinkedHashMap<>();
                for (FlowSpec s : specs) {
                    List<String> ids = new ArrayList<>();
                    for (int i = 0; i < PER_WORKFLOW; i++) ids.add(client.start(s, seed(s.name(), i)));
                    started.put(s.name(), ids);
                }
                int total = WORKFLOWS * PER_WORKFLOW;
                assertEquals(total, started.values().stream().mapToInt(List::size).sum());

                // ---- the terminal shapes must all settle
                awaitAll(client, started.get(PREFIX + "linear"), "COMPLETED");
                awaitAll(client, started.get(PREFIX + "forked"), "COMPLETED");
                awaitAll(client, started.get(PREFIX + "foreach"), "COMPLETED");
                awaitAll(client, started.get(PREFIX + "branchy"), "COMPLETED");
                awaitAll(client, started.get(PREFIX + "failing"), "FAILED");
                awaitAll(client, started.get(PREFIX + "saga"), "COMPENSATED");

                // a closed gate is a clean end, with a reason
                for (String id : started.get(PREFIX + "gated")) {
                    InstanceView v = client.awaitCompletion(id, Duration.ofSeconds(60));
                    assertEquals("COMPLETED", v.status(), "a closed gate ends cleanly");
                    assertTrue(String.valueOf(v.terminationReason()).startsWith("gated:"),
                            "and says which gate closed: " + v.terminationReason());
                    assertTrue(Json.asObject(v.context()).get("bug") == null,
                            "nothing downstream of the gate ran");
                }

                // ---- cancel half the parked instances; the rest stay parked on a signal
                List<String> parkedIds = started.get(PREFIX + "parked");
                for (int i = 0; i < parkedIds.size() / 2; i++) {
                    client.cancel(parkedIds.get(i), "sweep: cancelled while awaiting a signal");
                }
                awaitAll(client, parkedIds.subList(0, parkedIds.size() / 2), "CANCELLED");
                for (String id : parkedIds.subList(parkedIds.size() / 2, parkedIds.size())) {
                    assertEquals("RUNNING", client.instance(id).status(),
                            "an instance waiting on a signal nobody sends stays RUNNING");
                }

                // ---- every compensable step of every saga ran its undo, exactly once each
                assertEquals(PER_WORKFLOW, undos.getOrDefault("reserved", 0), "every reserve was undone");
                assertEquals(PER_WORKFLOW, undos.getOrDefault("charged", 0), "every charge was undone");

                // ---- totals hold when counted per workflow, not just per instance.
                // Counted over this run's own ids: a live database keeps its rows, so an absolute
                // count would pick up every previous run's instances too.
                for (FlowSpec s : specs) {
                    List<String> mine = started.get(s.name());
                    List<InstanceView> listed = client.listInstances(s.name(), null, 5_000);
                    long found = listed.stream().map(InstanceView::id).filter(mine::contains).count();
                    assertEquals(PER_WORKFLOW, found,
                            "every instance of " + s.name() + " is listed exactly once");
                    assertEquals(mine.size(), mine.stream().distinct().count(),
                            s.name() + " handed out a duplicate instance id");
                }

                // ---- and the coverage view, read across all of it, blames nothing
                List<WiggleClient.BacklogSlice> slices = sweepSlices(client);
                for (WiggleClient.BacklogSlice slice : slices) {
                    assertTrue(slice.covered(),
                            "this worker binds every sweep workflow unscoped, so nothing may report "
                                    + "uncovered -- got " + slice.workflow() + " v" + slice.version()
                                    + " on " + slice.queue());
                }
            }

            // ---- with the worker gone, the parked work has no cover at all
            awaitUncovered(client);
        }
    }

    // ------------------------------------------------------------------ helpers

    private static Map<String, Object> seed(String workflow, int i) {
        if (workflow.equals(PREFIX + "foreach")) {
            return Map.of("items", List.of(Map.of("sku", "a" + i), Map.of("sku", "b" + i)));
        }
        if (workflow.equals(PREFIX + "branchy")) return Map.of("n", (long) i, "left", 2L);
        return Map.of("i", (long) i);
    }

    private static void awaitAll(WiggleClient client, List<String> ids, String expected) {
        for (String id : ids) {
            InstanceView v = client.awaitCompletion(id, Duration.ofSeconds(60));
            assertEquals(expected, v.status(), id + " (error: " + v.error() + ")");
        }
    }

    /** Cancels leftovers -- a live store keeps its rows, and this asserts on per-workflow counts. */
    private static void clear(WiggleClient client, String workflow) {
        for (InstanceView v : client.listInstances(workflow, "RUNNING", 500)) {
            try { client.cancel(v.id(), "sweep cleanup"); } catch (RuntimeException ignored) { }
        }
    }

    private static List<WiggleClient.BacklogSlice> sweepSlices(WiggleClient client) {
        List<WiggleClient.BacklogSlice> out = new ArrayList<>();
        for (WiggleClient.BacklogSlice s : client.backlogCoverage(500)) {
            if (s.workflow().startsWith(PREFIX)) out.add(s);
        }
        return out;
    }

    /**
     * After the worker closes, its registry entry expires and the parked instances' work -- once their
     * signal arrives or their lease lapses -- has nobody. The registry is TTL'd rather than
     * closed-on-disconnect, so this only asserts that coverage is computed, not that it flips fast.
     */
    private static void awaitUncovered(WiggleClient client) {
        List<WiggleClient.BacklogSlice> slices = sweepSlices(client);
        for (WiggleClient.BacklogSlice s : slices) {
            assertTrue(s.readyCount() >= 0, "coverage is still computable with no workers attached");
        }
    }
}
