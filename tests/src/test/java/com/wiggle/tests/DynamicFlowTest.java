package com.wiggle.tests;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.CompensableActivity;
import com.wiggle.client.worker.Compensation;
import com.wiggle.client.worker.PermanentActivityException;
import com.wiggle.client.worker.ForFlow;
import com.wiggle.client.worker.Step;
import com.wiggle.client.worker.Worker;
import com.wiggle.core.ExecutionMode;
import com.wiggle.core.Ids;
import com.wiggle.core.InstanceView;
import com.wiggle.core.Json;
import com.wiggle.core.Node;
import com.wiggle.core.NodeKind;
import com.wiggle.core.RetryPolicy;
import com.wiggle.core.ScratchKeys;
import com.wiggle.dist.WiggleStorageFactory;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Branches a step creates at run time with {@code Step.create(input).thenApply(...)}: each runs on
 * its own input, the combine after the step receives their results, and a combine that creates
 * branches runs another round. See docs/spec/35-dynamic-flows.md.
 */
class DynamicFlowTest {

    interface Steps {
        Map<String, Object> fulfil(Map<String, Object> order);
        Map<String, Object> summarise(Map<String, Object> base, List<Map<String, Object>> results);
    }

    interface KeyedSteps {
        Map<String, Object> fulfil(Map<String, Object> order);
        Map<String, Object> summarise(Map<String, Map<String, Object>> results);
    }

    interface RoundSteps {
        Map<String, Object> begin(Map<String, Object> ctx);
        Map<String, Object> next(Map<String, Object> base, List<Map<String, Object>> results);
    }

    interface SagaSteps {
        Map<String, Object> fulfil(Map<String, Object> order);
        Map<String, Object> summarise(List<Map<String, Object>> results);
        Map<String, Object> boom(Map<String, Object> ctx);
    }

    interface PlainSteps {
        Map<String, Object> plain(Map<String, Object> ctx);
    }

    private static Map<String, Object> put(Map<String, Object> ctx, String k, Object v) {
        Map<String, Object> n = new LinkedHashMap<>(ctx);
        n.put(k, v);
        return n;
    }

    @ForFlow("dyn-flow")
    public static final class FulfilH {
        public Map<String, Object> fulfil(Map<String, Object> order) {
            for (Object o : (List<?>) order.get("items")) {
                String item = (String) o;
                Map<String, Object> input = Map.of("item", item);
                if (item.startsWith("digital")) {
                    Step.create(input).thenApply(this::link);
                } else {
                    Step.create(input).thenApply(this::reserve).thenFilter(this::inStock).thenApply(this::ship);
                }
            }
            return put(order, "fulfilling", true);
        }

        public Map<String, Object> link(Map<String, Object> v) { return put(v, "via", "link"); }

        public Map<String, Object> reserve(Map<String, Object> v) { return put(v, "reserved", true); }

        public boolean inStock(Map<String, Object> v) { return !String.valueOf(v.get("item")).contains("gone"); }

        public Map<String, Object> ship(Map<String, Object> v) { return put(v, "via", "ship"); }

        public Map<String, Object> summarise(Map<String, Object> base, List<Map<String, Object>> results) {
            List<String> summary = new ArrayList<>();
            for (Map<String, Object> r : results) summary.add(r.get("item") + ":" + r.get("via"));
            return put(put(base, "summary", summary), "base-seen", base.get("fulfilling"));
        }
    }

    private static FlowSpec fulfilment(ExecutionMode mode) {
        return FlowSpec.define("dyn-flow", 1, Map.class, Steps.class, (f, s) -> Modes.in(f, mode)
                .thenApply(s::fulfil)
                .combine(s::summarise));
    }

    @Test @DisplayName("the topology: a combine directly after a step makes that step create branches")
    void compilesToStepJoinCombine() {
        FlowSpec spec = fulfilment(ExecutionMode.SERVER);
        Map<String, Node> nodes = spec.definition().nodes();
        Node fulfil = nodes.values().stream().filter(n -> "fulfil".equals(n.name())).findFirst().orElseThrow();
        Node join = nodes.get(fulfil.next());
        Node combine = nodes.get(join.next());
        assertEquals(NodeKind.JOIN, join.kind());
        assertEquals(0, join.expected(), "a dynamic barrier: the width travels with the branches");
        assertEquals(ScratchKeys.spawn("fulfil"), combine.collectKey());
        assertEquals("summarise", combine.name());
    }

    @Test @DisplayName("a combine must directly follow a task")
    void combineNeedsATask() {
        IllegalStateException e = assertThrows(IllegalStateException.class, () ->
                FlowSpec.define("dyn-bad", 1, Map.class, Steps.class, (f, s) -> f
                        .thenSleep(Duration.ofSeconds(1))
                        .combine(s::summarise)));
        assertTrue(e.getMessage().contains("must directly follow"), e.getMessage());
    }

    @Test @DisplayName("each branch runs its own chain; the combine gets their results in creation order")
    void branchesRunTheirOwnChains() throws Exception {
        for (ExecutionMode mode : new ExecutionMode[]{ExecutionMode.SERVER, ExecutionMode.LOCAL_SYNC}) {
            InstanceView v = run(fulfilment(mode), new FulfilH(),
                    Map.of("items", List.of("book", "digital-song", "chair-gone")), null);
            assertEquals("COMPLETED", v.status(), mode + " " + v.error());
            Map<String, Object> ctx = Json.asObject(v.context());
            assertEquals(List.of("book:ship", "digital-song:link", "chair-gone:null"), ctx.get("summary"),
                    mode + " a false gate ends its branch at the join with the view it had");
            assertEquals(true, ctx.get("base-seen"), mode + " the base is what the creating step returned");
            assertFalse(ctx.keySet().stream().anyMatch(k -> k.startsWith("__")), mode + " staged keys are stripped");
        }
    }

    @Test @DisplayName("branches over JDBC: created nodes are stored with the instance")
    void branchesOverJdbc() throws Exception {
        InstanceView v = run(fulfilment(ExecutionMode.SERVER), new FulfilH(),
                Map.of("items", List.of("book", "digital-song")), TestStorage.url("dynflow"));
        assertEquals("COMPLETED", v.status(), v.error());
        assertEquals(List.of("book:ship", "digital-song:link"), Json.asObject(v.context()).get("summary"));
    }

    @Test @DisplayName("a step that creates nothing still runs its combine, with an empty collection")
    void noBranchesStillCombines() throws Exception {
        InstanceView v = run(fulfilment(ExecutionMode.SERVER), new FulfilH(), Map.of("items", List.of()), null);
        assertEquals("COMPLETED", v.status(), v.error());
        assertEquals(List.of(), Json.asObject(v.context()).get("summary"));
    }

    @ForFlow("dyn-keyed")
    public static final class KeyedH {
        public Map<String, Object> fulfil(Map<String, Object> order) {
            for (Object o : (List<?>) order.get("items")) {
                String item = (String) o;
                Step.create(item, Map.<String, Object>of("item", item)).thenApply(this::price);
            }
            return order;
        }

        public Map<String, Object> price(Map<String, Object> v) {
            return put(v, "price", (long) String.valueOf(v.get("item")).length());
        }

        public Map<String, Object> summarise(Map<String, Map<String, Object>> results) {
            Map<String, Object> prices = new LinkedHashMap<>();
            results.forEach((k, r) -> prices.put(k, r.get("price")));
            return Map.of("prices", prices);
        }
    }

    @Test @DisplayName("keyed branches reach the combine as a map by key")
    void keyedBranches() throws Exception {
        FlowSpec spec = FlowSpec.define("dyn-keyed", 1, Map.class, KeyedSteps.class, (f, s) -> f
                .thenApply(s::fulfil)
                .combine(s::summarise));
        InstanceView v = run(spec, new KeyedH(), Map.of("items", List.of("pen", "lamp")), null);
        assertEquals("COMPLETED", v.status(), v.error());
        assertEquals(Map.of("pen", 3L, "lamp", 4L), Json.asObject(v.context()).get("prices"));
    }

    @ForFlow("dyn-rounds")
    public static final class RoundsH {
        public Map<String, Object> begin(Map<String, Object> ctx) {
            Step.create(Map.<String, Object>of("n", 0L)).thenApply(this::inc);
            return put(ctx, "rounds", 1L);
        }

        public Map<String, Object> inc(Map<String, Object> v) {
            return Map.of("n", ((Number) v.get("n")).longValue() + 1);
        }

        public Map<String, Object> next(Map<String, Object> base, List<Map<String, Object>> results) {
            long n = ((Number) results.getFirst().get("n")).longValue();
            if (n < ((Number) base.get("target")).longValue()) {
                Step.create(Map.<String, Object>of("n", n)).thenApply(this::inc);
                return put(base, "rounds", ((Number) base.get("rounds")).longValue() + 1);
            }
            return put(base, "final", n);
        }
    }

    private static FlowSpec rounds() {
        return FlowSpec.define("dyn-rounds", 1, Map.class, RoundSteps.class, (f, s) -> f
                .thenApply(s::begin)
                .combine(s::next));
    }

    @Test @DisplayName("a combine that creates branches runs another round over the base it returned")
    void combineRunsRounds() throws Exception {
        InstanceView v = run(rounds(), new RoundsH(), Map.of("target", 3L), null);
        assertEquals("COMPLETED", v.status(), v.error());
        Map<String, Object> ctx = Json.asObject(v.context());
        assertEquals(3L, ctx.get("final"));
        assertEquals(3L, ctx.get("rounds"), "each round's base is what the combine returned");
    }

    @Test @DisplayName("rounds are budgeted: one past the budget fails the instance")
    void roundBudget() throws Exception {
        System.setProperty("wiggle.dyn.maxRounds", "2");
        try {
            InstanceView v = run(rounds(), new RoundsH(), Map.of("target", 5L), null);
            assertEquals("FAILED", v.status());
            assertTrue(v.error().contains("WIGGLE_DYN_MAX_ROUNDS"), v.error());
        } finally {
            System.clearProperty("wiggle.dyn.maxRounds");
        }
    }

    @ForFlow("dyn-saga")
    public static final class SagaH {
        final AtomicInteger undone = new AtomicInteger();

        public Map<String, Object> fulfil(Map<String, Object> order) {
            for (Object item : (List<?>) order.get("items")) {
                Step.create(Map.<String, Object>of("item", item)).thenApplyCompensable(this::charge);
            }
            return order;
        }

        public CompensableActivity<Map<String, Object>, Map<String, Object>> charge() {
            AtomicInteger counter = undone;
            return new CompensableActivity<>() {
                public Map<String, Object> execute(Map<String, Object> v) { return put(v, "charged", true); }

                public void compensate(Compensation<Map<String, Object>, Map<String, Object>> c) {
                    counter.incrementAndGet();
                }
            };
        }

        public Map<String, Object> summarise(List<Map<String, Object>> results) {
            return Map.of("charged", (long) results.size());
        }

        public Map<String, Object> boom(Map<String, Object> ctx) {
            throw new PermanentActivityException("deliberate: the charges must be undone");
        }
    }

    @Test @DisplayName("a compensable step in a created branch is undone when the instance later fails")
    void branchStepsCompensate() throws Exception {
        FlowSpec spec = FlowSpec.define("dyn-saga", 1, Map.class, SagaSteps.class, (f, s) -> f
                .thenApply(s::fulfil)
                .combine(s::summarise)
                .thenApply(s::boom));
        SagaH handlers = new SagaH();
        InstanceView v = run(spec, handlers, Map.of("items", List.of("a", "b")), null);
        assertEquals("COMPENSATED", v.status(), v.error());
        assertEquals(2, handlers.undone.get(), "each branch's charge was undone");
    }

    record Link(String item, String url) {}

    record Shipment(String item, String tracking) {}

    interface SplitSteps {
        Map<String, Object> fulfil(Map<String, Object> order);
        Map<String, Object> summarise(List<Shipment> shipped, Map<String, Object> order, List<Link> links);
    }

    @ForFlow("dyn-split")
    public static final class SplitH {
        public Map<String, Object> fulfil(Map<String, Object> order) {
            for (Object o : (List<?>) order.get("items")) {
                String item = (String) o;
                Map<String, Object> input = Map.of("item", item);
                if (item.startsWith("digital")) Step.create(input).thenApply(this::link);
                else Step.create(input).thenApply(this::ship);
            }
            return order;
        }

        public Link link(Map<String, Object> v) { return new Link((String) v.get("item"), "https://dl/" + v.get("item")); }

        public Shipment ship(Map<String, Object> v) { return new Shipment((String) v.get("item"), "T-" + v.get("item")); }

        public Map<String, Object> summarise(List<Shipment> shipped, Map<String, Object> order, List<Link> links) {
            return Map.of("order", order.get("id"),
                    "shipped", shipped.stream().map(Shipment::tracking).toList(),
                    "links", links.stream().map(Link::url).toList());
        }
    }

    @Test @DisplayName("a combine splits the results by the type each branch's last step produces")
    void combineSplitsResultsByType() throws Exception {
        FlowSpec spec = FlowSpec.define("dyn-split", 1, Map.class, SplitSteps.class, (f, s) -> f
                .thenApply(s::fulfil)
                .combine(s::summarise));
        InstanceView v = run(spec, new SplitH(),
                Map.of("id", "o-1", "items", List.of("book", "digital-song", "chair")), null);
        assertEquals("COMPLETED", v.status(), v.error());
        Map<String, Object> ctx = Json.asObject(v.context());
        assertEquals(List.of("T-book", "T-chair"), ctx.get("shipped"), "creation order within a type");
        assertEquals(List.of("https://dl/digital-song"), ctx.get("links"));
        assertEquals("o-1", ctx.get("order"), "the parameter that is not a collection is the base");
    }

    interface NestSteps {
        Map<String, Object> fulfil(Map<String, Object> order);
        Map<String, Object> summarise(List<Map<String, Object>> lines);
    }

    @ForFlow("dyn-nest")
    public static final class NestH {
        public Map<String, Object> fulfil(Map<String, Object> order) {
            for (Object line : (List<?>) order.get("lines")) {
                @SuppressWarnings("unchecked") Map<String, Object> l = (Map<String, Object>) line;
                Step.create(l).thenApply(this::pick).combine(this::packed).thenApply(this::ship);
            }
            return order;
        }

        /** A created step that creates branches of its own: one per unit of the line. */
        public Map<String, Object> pick(Map<String, Object> line) {
            long qty = ((Number) line.get("qty")).longValue();
            for (long u = 0; u < qty; u++) {
                Step.create(Map.<String, Object>of("unit", u)).thenApply(this::scan);
            }
            return put(line, "picked", true);
        }

        public Map<String, Object> scan(Map<String, Object> unit) {
            return put(unit, "sku", Step.base().get("sku"));   // the inner base is the picked line
        }

        public Map<String, Object> packed(List<Map<String, Object>> units, Map<String, Object> line) {
            List<String> labels = new ArrayList<>();
            for (Map<String, Object> u : units) labels.add(u.get("sku") + "#" + u.get("unit"));
            return put(line, "labels", labels);
        }

        public Map<String, Object> ship(Map<String, Object> line) { return put(line, "shipped", true); }

        public Map<String, Object> summarise(List<Map<String, Object>> lines) {
            List<Object> out = new ArrayList<>();
            for (Map<String, Object> l : lines) out.add(l.get("labels") + ":" + l.get("shipped"));
            return Map.of("lines", out);
        }
    }

    private static FlowSpec nested(ExecutionMode mode) {
        return FlowSpec.define("dyn-nest", 1, Map.class, NestSteps.class, (f, s) -> Modes.in(f, mode)
                .thenApply(s::fulfil)
                .combine(s::summarise));
    }

    private static final Map<String, Object> TWO_LINES = Map.of("lines", List.of(
            Map.of("sku", "pen", "qty", 2L), Map.of("sku", "lamp", "qty", 1L)));

    @Test @DisplayName("a created step creates branches of its own; its combine continues the outer branch")
    void branchesNest() throws Exception {
        for (ExecutionMode mode : new ExecutionMode[]{ExecutionMode.SERVER, ExecutionMode.LOCAL_SYNC}) {
            InstanceView v = run(nested(mode), new NestH(), TWO_LINES, null);
            assertEquals("COMPLETED", v.status(), mode + " " + v.error());
            assertEquals(List.of("[pen#0, pen#1]:true", "[lamp#0]:true"), Json.asObject(v.context()).get("lines"),
                    mode + " each line's units were scanned against it, packed, then the line shipped");
        }
    }

    @Test @DisplayName("nested branches over JDBC")
    void branchesNestOverJdbc() throws Exception {
        InstanceView v = run(nested(ExecutionMode.SERVER), new NestH(), TWO_LINES, TestStorage.url("dynnest"));
        assertEquals("COMPLETED", v.status(), v.error());
        assertEquals(List.of("[pen#0, pen#1]:true", "[lamp#0]:true"), Json.asObject(v.context()).get("lines"));
    }

    @Test @DisplayName("nesting is bounded by depth and by the instance's created nodes")
    void nestingIsBounded() throws Exception {
        for (String[] limit : new String[][]{{"wiggle.dyn.maxDepth", "1", "WIGGLE_DYN_MAX_DEPTH"},
                                              {"wiggle.dyn.maxNodes", "9", "WIGGLE_DYN_MAX_NODES"}}) {   // the lines take 8; a line's units pass 9
            System.setProperty(limit[0], limit[1]);
            try {
                InstanceView v = run(nested(ExecutionMode.SERVER), new NestH(), TWO_LINES, null);
                assertEquals("FAILED", v.status(), limit[0]);
                assertTrue(v.error().contains(limit[2]), v.error());
            } finally {
                System.clearProperty(limit[0]);
            }
        }
    }

    @ForFlow("dyn-plain")
    public static final class PlainH {
        public Map<String, Object> plain(Map<String, Object> ctx) {
            Step.create(ctx).thenApply(this::plain);
            return ctx;
        }
    }

    @Test @DisplayName("a step without a combine after it cannot create branches")
    void onlyASpawningStepCreates() throws Exception {
        FlowSpec spec = FlowSpec.define("dyn-plain", 1, RetryPolicy.none(), Map.class, PlainSteps.class,
                (f, s) -> f.thenApply(s::plain));
        InstanceView v = run(spec, new PlainH(), Map.of(), null);
        assertEquals("FAILED", v.status());
        assertTrue(v.error().contains("cannot create branches"), v.error());
    }

    private static ServerConfig config(String jdbcUrl) {
        return new ServerConfig(TestPorts.free(), "dynflow-node", jdbcUrl,
                jdbcUrl == null ? null : TestStorage.user(), jdbcUrl == null ? null : TestStorage.password(), 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    private InstanceView run(FlowSpec spec, Object handlers, Map<String, Object> input, String jdbcUrl)
            throws Exception {
        try (WiggleServer server = new WiggleServer(config(jdbcUrl), new WiggleStorageFactory()).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker w = new Worker(client, "dynflow-" + Ids.next("x")).registerHandler(handlers)) {
            client.register(spec);
            w.start();
            return client.awaitCompletion(client.start(spec, new LinkedHashMap<>(input)), Duration.ofSeconds(20));
        }
    }
}
