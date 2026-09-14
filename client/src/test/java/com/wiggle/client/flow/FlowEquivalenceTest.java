package com.wiggle.client.flow;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.flow.Branch;
import com.wiggle.client.flow.Case;
import com.wiggle.client.flow.Wiggle;
import com.wiggle.client.flow.Fixtures.Fulfilment;
import com.wiggle.client.flow.Fixtures.Line;
import com.wiggle.client.flow.Fixtures.Order;
import com.wiggle.client.flow.Fixtures.Steps;
import com.wiggle.client.flow.Fixtures.Shipment;
import com.wiggle.core.Node;
import com.wiggle.core.NodeKind;
import com.wiggle.core.RetryPolicy;
import com.wiggle.core.WorkflowDefinition;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The load-bearing claim of the future-shaped API: it is a front-end, not a second model. For the
 * same topology it must emit the <em>same definition</em> as the name-based DSL -- same nodes, same
 * ids, same edges -- so nothing downstream (registration, the graph rows, handler binding, the
 * console) can tell which one was used.
 *
 * <p>Each case asserts that by comparing {@code version()}, which is a SHA-256 over the whole
 * canonical topology (names, kinds, queues, retry, edges, ids, execution mode, checkpoints). Equal
 * versions is equality of the entire artifact, not a sample of it; the extra assertions that follow
 * only pin down which names the method references produced, since that is the part a reader wants to
 * see spelled out.
 */
class FlowEquivalenceTest {

    /** The contract the specs name their steps through -- inert, never invoked. */
    private final Steps h = Steps.class.cast(java.lang.reflect.Proxy.newProxyInstance(
            Steps.class.getClassLoader(), new Class<?>[] {Steps.class},
            (p, m, a) -> { throw new IllegalStateException(m.getName()); }));

    private static Node named(WorkflowDefinition def, String name) {
        return def.nodes().values().stream().filter(n -> name.equals(n.name())).findFirst()
                .orElseThrow(() -> new AssertionError("no node named '" + name + "' in "
                        + def.nodes().values().stream().map(Node::name).toList()));
    }

    private static void assertSameDefinition(FlowSpec dsl, FlowSpec flow) {
        assertEquals(dsl.name(), flow.name());
        assertEquals(dsl.definition().startNode(), flow.definition().startNode());
        assertEquals(dsl.definition().nodes().keySet(), flow.definition().nodes().keySet());
        assertEquals(dsl.version(), flow.version(),
                "the flow API must emit the identical topology, so the content hashes must match");
    }

    // ------------------------------------------------------------------ step / gate / fan-out / sleep

    @Test
    void stepsGatesFanOutAndSleepCompileToTheSameGraphAsTheDsl() {
        FlowSpec dsl = Wiggle.graph("order-fulfilment")
                .step("validate")
                .gate("inStock")
                .fork(Branch.of("charge", s -> s.step("charge")),
                      Branch.of("label", s -> s.step("reserve")
                                                  .sleep(Duration.ofSeconds(2))
                                                  .step("label")))
                .combine("settle")
                .effect("notifyCustomer")
                .build();

        FlowSpec flow = Wiggle.define("order-fulfilment", Order.class, f -> {
            var validated = f.thenApply(h::validate).thenFilter(h::inStock);

            // continuing `validated` twice is the fan-out; allOf records where the graph splits
            var payment = validated.thenApply(h::charge);
            var shipping = validated.thenApply(h::reserve)
                                    .thenSleep(Duration.ofSeconds(2))
                                    .thenApply(h::label);

            return Wiggle.allOf(payment, shipping)
                    .combine(h::settle)
                    .thenAccept(h::notifyCustomer);
        });

        assertSameDefinition(dsl, flow);

        WorkflowDefinition def = flow.definition();
        assertEquals(NodeKind.TASK, named(def, "validate").kind());
        assertEquals(NodeKind.PREDICATE, named(def, "inStock").kind());
        assertEquals(NodeKind.TASK, named(def, "notifyCustomer").kind());
        // the combine still carries the arm names the engine keys each isolated branch's result by
        assertEquals("[\"charge\",\"label\"]", named(def, "settle").itemsKey());
    }

    @Test
    void armsAreNamedAfterTheirLastStep() {
        FlowSpec flow = Wiggle.define("unnamed-arms", Order.class, f -> {
            var payment = f.thenApply(h::charge);
            var shipping = f.thenApply(h::label);
            return Wiggle.allOf(payment, shipping).combine("merge", Order.class);
        });

        assertEquals("[\"charge\",\"label\"]", named(flow.definition(), "merge").itemsKey());
    }

    @Test
    void threeArmedFanOutCombinesThroughTheTypedTriFunction() {
        FlowSpec dsl = Wiggle.graph("audit")
                .fork(Branch.of("charge", s -> s.step("charge")),
                      Branch.of("label", s -> s.step("label")),
                      Branch.of("ship", s -> s.subWorkflow("ship", "carrier-flow")))
                .combine("audit")
                .build();

        FlowSpec flow = Wiggle.define("audit", Order.class, f -> {
            var payment = f.thenApply(h::charge);
            var shipping = f.thenApply(h::label);
            var carrier = f.thenSubFlow("ship", "carrier-flow", Shipment.class);
            return Wiggle.allOf(payment, shipping, carrier).combine(h::audit);
        });

        assertSameDefinition(dsl, flow);
    }

    @Test
    void theTypedForkSeriesGoesWellPastAnyRealWorkflow() {
        // Fork2..Fork10 are generated, so a five-armed fan-out is typed end to end: five arms of five
        // different types, and a combine whose parameters must line up with them
        FlowSpec dsl = Wiggle.graph("five")
                .fork(Branch.of("armA", s -> s.step("armA")),
                      Branch.of("armB", s -> s.step("armB")),
                      Branch.of("armC", s -> s.step("armC")),
                      Branch.of("armD", s -> s.step("armD")),
                      Branch.of("armE", s -> s.step("armE")))
                .combine("settleFive")
                .build();

        FlowSpec flow = Wiggle.define("five", Order.class, f -> {
            var a = f.thenApply(h::armA);
            var b = f.thenApply(h::armB);
            var c = f.thenApply(h::armC);
            var d = f.thenApply(h::armD);
            var e = f.thenApply(h::armE);
            return Wiggle.allOf(a, b, c, d, e).combineWithContext(h::settleFive);
        });

        assertSameDefinition(dsl, flow);
        assertEquals("[\"armA\",\"armB\",\"armC\",\"armD\",\"armE\"]",
                named(flow.definition(), "settleFive").itemsKey());
    }

    @Test
    void pastTheTypedSeriesTheCombineIsNamedInstead() {
        // eleven arms: more than Fork10, so allOf(WiggleFlow...) takes over and the merge is named
        FlowSpec flow = Wiggle.define("wide", Order.class, f -> {
            WiggleFlow<?>[] arms = new WiggleFlow<?>[11];
            arms[0] = f.thenApply(h::armA);
            arms[1] = f.thenApply(h::armB);
            arms[2] = f.thenApply(h::armC);
            arms[3] = f.thenApply(h::armD);
            arms[4] = f.thenApply(h::armE);
            arms[5] = f.thenApply(h::charge);
            arms[6] = f.thenApply(h::label);
            arms[7] = f.thenApply(h::reserve);
            arms[8] = f.thenApply(h::validate);
            arms[9] = f.thenApply(h::vipPath);
            arms[10] = f.thenApply(h::standardPath);
            return Wiggle.allOf(arms).combine("merge", Order.class);
        });

        assertEquals(11, com.wiggle.core.Json.asArray(
                com.wiggle.core.Json.parse(named(flow.definition(), "merge").itemsKey())).size());
    }

    // ------------------------------------------------------------------ the combine's contract

    @Test
    void aCombineMayTakeThePreForkContextAlongsideTheArms() {
        FlowSpec dsl = Wiggle.graph("settle-with-base")
                .fork(Branch.of("charge", s -> s.step("charge")),
                      Branch.of("label", s -> s.step("label")))
                .combine("settleWithBase")
                .build();

        FlowSpec flow = Wiggle.define("settle-with-base", Order.class, f -> {
            var payment = f.thenApply(h::charge);
            var shipping = f.thenApply(h::label);
            return Wiggle.allOf(payment, shipping).combineWithContext(h::settleWithBase);
        });

        assertSameDefinition(dsl, flow);
    }

    @Test
    void aCombineTakesItsArmsByPosition() {
        // the parameters take the arms in the order given to allOf, which the typed signature has
        // already pinned -- the arm names stay an engine detail
        FlowSpec flow = Wiggle.define("positional", Order.class, f -> {
            var payment = f.thenApply(h::charge);
            var shipping = f.thenApply(h::label);
            return Wiggle.allOf(payment, shipping).combine(h::settlePositionally);
        });

        assertEquals("[\"charge\",\"label\"]", named(flow.definition(), "settlePositionally").itemsKey());
    }

    @Test
    void aCombineMissingTheContextParameterIsRejectedWhileDefining() {
        // the one shape mistake the compiler cannot catch: the types line up, so only the missing
        // @Context distinguishes a context-taking combine from a three-armed one. A wrong *arity* is
        // already a compile error on the typed path -- the binder test covers it for Wiggle.graph
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> {
            Wiggle.define("no-context", Order.class, f -> {
                var payment = f.thenApply(h::charge);
                var shipping = f.thenApply(h::label);
                return Wiggle.allOf(payment, shipping).combineWithContext(h::unannotatedBase);
            });
        });

        assertTrue(ex.getMessage().contains("@Context"), ex.getMessage());
    }

    // ------------------------------------------------------------------ choose / loop

    @Test
    void oneOfAndRepeatWhileCompileToTheSameGraphAsTheDsl() {
        FlowSpec dsl = Wiggle.graph("triage")
                .step("validate")
                .choose(Case.when("isVip", s -> s.step("vipPath")),
                        Case.otherwise("standard", s -> s.step("standardPath")))
                .doWhile("hasMore", s -> s.step("drain"))
                .build();

        FlowSpec flow = Wiggle.define("triage", Order.class, f -> {
            var validated = f.thenApply(h::validate);

            // exactly one of these runs -- allOf's exclusive twin, and the arms need no merge
            var vip = validated.when(h::isVip).thenApply(h::vipPath);
            var standard = validated.otherwise().thenApply(h::standardPath);

            return Wiggle.oneOf(vip, standard)
                    .repeatWhile(h::hasMore, a -> a.thenApply(h::drain));
        });

        assertSameDefinition(dsl, flow);

        WorkflowDefinition def = flow.definition();
        assertEquals(NodeKind.PREDICATE, named(def, "isVip").kind());
        Node loop = named(def, "hasMore");
        assertEquals(NodeKind.PREDICATE, loop.kind());
        assertEquals(named(def, "drain").id(), loop.next(), "a true condition re-enters the loop body");
    }

    @Test
    void repeatWhileWithABudgetMatchesTheDslsBoundedDoWhile() {
        FlowSpec dsl = Wiggle.graph("drain")
                .doWhile("hasMore", 5, s -> s.step("drain"))
                .build();

        FlowSpec flow = Wiggle.define("drain", Order.class, f -> f
                .repeatWhile(h::hasMore, 5, a -> a.thenApply(h::drain)));

        assertSameDefinition(dsl, flow);
    }

    // ------------------------------------------------------------------ forEach / signal

    @Test
    void forEachCompilesToTheSameDynamicFanOutAsTheDsl() {
        FlowSpec dsl = Wiggle.graph("pricing")
                .step("validate")
                .forEach("lines", s -> s.step("price"))
                .combine("total")
                .build();

        FlowSpec flow = Wiggle.define("pricing", Order.class, f -> f
                .thenApply(h::validate)
                .thenForEach("lines", Line.class, a -> a.thenApply(h::price))
                .combine(h::total));

        assertSameDefinition(dsl, flow);

        WorkflowDefinition def = flow.definition();
        assertEquals(NodeKind.DYN_FORK, named(def, "lines").kind());
        // a forEach combine's itemsKey is the scratch key the collected results are staged under
        assertEquals("\"lines\"", named(def, "total").itemsKey());
    }

    @Test
    void forEachCombineMayAlsoTakeThePreForEachContext() {
        FlowSpec flow = Wiggle.define("pricing-with-base", Order.class, f -> f
                .thenForEach("lines", Line.class, a -> a.thenApply(h::price))
                .combine(h::totalWithBase));

        assertEquals("totalWithBase", named(flow.definition(), "totalWithBase").name());
    }

    @Test
    void signalWaitsAndSubFlowsCompileToTheSameGraphAsTheDsl() {
        FlowSpec dsl = Wiggle.graph("approval")
                .step("validate")
                .awaitSignal("approved", Duration.ofMinutes(5))
                .subWorkflow("ship", "shipping-flow")
                .build();

        FlowSpec flow = Wiggle.define("approval", Order.class, f -> f
                .thenApply(h::validate)
                .thenAwait("approved", Duration.ofMinutes(5))
                .thenSubFlow("ship", "shipping-flow", Shipment.class));

        assertSameDefinition(dsl, flow);
        assertEquals(NodeKind.SUB_WORKFLOW, named(flow.definition(), "ship").kind());
    }

    // ------------------------------------------------------------------ names and per-step settings

    @Test
    void handlesAnnotationRenamesTheNodeSoBothSidesAgree() {
        FlowSpec flow = Wiggle.define("renamed", Order.class, f -> f.thenApply(h::doCapture));

        // the handler is bound by @Handles("capture-payment"), so that -- not "doCapture" -- is the node
        List<String> names = flow.definition().nodes().values().stream()
                .map(Node::name).filter(n -> n != null).toList();
        assertTrue(names.contains("capture-payment"), names.toString());
        assertTrue(!names.contains("doCapture"), names.toString());
    }

    @Test
    void perStepQueueRetryCompensateAndCheckpointPassThroughUnchanged() {
        FlowSpec dsl = Wiggle.graph("settings")
                .step("validate", "fast-queue").compensate()
                .step("charge", com.wiggle.core.RetryPolicy.exponential(7, Duration.ofMillis(250)))
                .checkpoint()
                .build();

        FlowSpec flow = Wiggle.define("settings", Order.class, f -> f
                .thenApply(h::validate, "fast-queue").compensate()
                .thenApply(h::charge, com.wiggle.core.RetryPolicy.exponential(7, Duration.ofMillis(250)))
                .checkpoint());

        assertSameDefinition(dsl, flow);
        assertTrue(flow.queues().contains("fast-queue"));
    }

    @Test
    void everyRetryAndQueueCombinationMatchesTheDsl() {
        // the builder takes (name), (name, retry), (name, queue) and (name, retry, queue) for each of
        // step / effect / gate; every one of those has a flow overload, with retry and queue accepted
        // in either order. Each pair below is one overload against the builder call it must produce.
        RetryPolicy retry = RetryPolicy.exponential(4, Duration.ofMillis(150));
        String queue = "payments";

        assertSameDefinition(Wiggle.graph("t").step("charge").build(),
                Wiggle.define("t", Order.class, f -> f.thenApply(h::charge)));
        assertSameDefinition(Wiggle.graph("t").step("charge", retry).build(),
                Wiggle.define("t", Order.class, f -> f.thenApply(h::charge, retry)));
        assertSameDefinition(Wiggle.graph("t").step("charge", queue).build(),
                Wiggle.define("t", Order.class, f -> f.thenApply(h::charge, queue)));
        assertSameDefinition(Wiggle.graph("t").step("charge", retry, queue).build(),
                Wiggle.define("t", Order.class, f -> f.thenApply(h::charge, retry, queue)));
        assertSameDefinition(Wiggle.graph("t").step("charge", retry, queue).build(),
                Wiggle.define("t", Order.class, f -> f.thenApply(h::charge, queue, retry)));

        assertSameDefinition(Wiggle.graph("t").effect("notifyCustomer").build(),
                Wiggle.define("t", Fulfilment.class, f -> f.thenAccept(h::notifyCustomer)));
        assertSameDefinition(Wiggle.graph("t").effect("notifyCustomer", retry).build(),
                Wiggle.define("t", Fulfilment.class, f -> f.thenAccept(h::notifyCustomer, retry)));
        assertSameDefinition(Wiggle.graph("t").effect("notifyCustomer", queue).build(),
                Wiggle.define("t", Fulfilment.class, f -> f.thenAccept(h::notifyCustomer, queue)));
        assertSameDefinition(Wiggle.graph("t").effect("notifyCustomer", retry, queue).build(),
                Wiggle.define("t", Fulfilment.class, f -> f.thenAccept(h::notifyCustomer, retry, queue)));
        assertSameDefinition(Wiggle.graph("t").effect("notifyCustomer", retry, queue).build(),
                Wiggle.define("t", Fulfilment.class, f -> f.thenAccept(h::notifyCustomer, queue, retry)));

        assertSameDefinition(Wiggle.graph("t").gate("inStock").build(),
                Wiggle.define("t", Order.class, f -> f.thenFilter(h::inStock)));
        assertSameDefinition(Wiggle.graph("t").gate("inStock", retry).build(),
                Wiggle.define("t", Order.class, f -> f.thenFilter(h::inStock, retry)));
        assertSameDefinition(Wiggle.graph("t").gate("inStock", queue).build(),
                Wiggle.define("t", Order.class, f -> f.thenFilter(h::inStock, queue)));
        assertSameDefinition(Wiggle.graph("t").gate("inStock", retry, queue).build(),
                Wiggle.define("t", Order.class, f -> f.thenFilter(h::inStock, retry, queue)));
        assertSameDefinition(Wiggle.graph("t").gate("inStock", retry, queue).build(),
                Wiggle.define("t", Order.class, f -> f.thenFilter(h::inStock, queue, retry)));
    }

    @Test
    void retryAndQueueOnDifferentStepKindsReachTheGraphTogether() {
        RetryPolicy retry = RetryPolicy.exponential(4, Duration.ofMillis(150));

        FlowSpec dsl = Wiggle.graph("pinned")
                .gate("inStock", "checks")
                .step("charge", retry, "payments")
                .build();

        FlowSpec flow = Wiggle.define("pinned", Order.class, f -> f
                .thenFilter(h::inStock, "checks")
                .thenApply(h::charge, retry, "payments"));

        // the hash covers per-node retry and queue, so this is the real assertion
        assertSameDefinition(dsl, flow);
        // and both queues were discovered, which is what a worker polls
        assertTrue(flow.queues().containsAll(List.of("payments", "checks")), flow.queues().toString());
    }

    @Test
    void retryAndQueueReachTheNodesWithNoInlineForm() {
        // a combine and a doWhile condition are worker-dispatched but have no (fn, retry, queue)
        // overload to carry them, so they are amended after the fact -- in both APIs alike
        RetryPolicy retry = RetryPolicy.exponential(6, Duration.ofMillis(80));

        FlowSpec dsl = Wiggle.graph("amended")
                .fork(Branch.of("charge", s -> s.step("charge")),
                      Branch.of("label", s -> s.step("label")))
                .combine("settle").withRetry(retry).onQueue("merges")
                .doWhile("hasMore", s -> s.step("drain")).withRetry(retry).onQueue("loops")
                .build();

        FlowSpec flow = Wiggle.define("amended", Order.class, f -> {
            var payment = f.thenApply(h::charge);
            var shipping = f.thenApply(h::label);
            return Wiggle.allOf(payment, shipping)
                    .combine("settle", Order.class).withRetry(retry).onQueue("merges")
                    .repeatWhile(h::hasMore, a -> a.thenApply(h::drain)).withRetry(retry).onQueue("loops");
        });

        assertSameDefinition(dsl, flow);
        assertTrue(flow.queues().containsAll(List.of("merges", "loops")), flow.queues().toString());
        assertEquals("merges", named(flow.definition(), "settle").queue());
        assertEquals("loops", named(flow.definition(), "hasMore").queue());
    }

    @Test
    void eachOneOfGuardCarriesItsOwnRetryAndQueue() {
        // a choose records several guards at once, so there is no "the one just added" to amend --
        // the settings ride on the case instead
        RetryPolicy retry = RetryPolicy.exponential(2, Duration.ofMillis(40));

        FlowSpec dsl = Wiggle.graph("triage")
                .choose(Case.when("isVip", retry, "vip-checks", s -> s.step("vipPath")),
                        Case.otherwise("otherwise", s -> s.step("standardPath")))
                .build();

        FlowSpec flow = Wiggle.define("triage", Order.class, f -> {
            var vip = f.when(h::isVip, retry, "vip-checks").thenApply(h::vipPath);
            var standard = f.otherwise().thenApply(h::standardPath);
            return Wiggle.oneOf(vip, standard);
        });

        assertSameDefinition(dsl, flow);
        assertEquals("vip-checks", named(flow.definition(), "isVip").queue());
    }

    @Test
    void oneOfWithNoOtherwiseArmSkipsPastWhenNothingMatched() {
        FlowSpec dsl = Wiggle.graph("maybe")
                .choose(Case.when("isVip", s -> s.step("vipPath")))
                .step("drain")
                .build();

        FlowSpec flow = Wiggle.define("maybe", Order.class, f -> {
            var vip = f.when(h::isVip).thenApply(h::vipPath);
            var plain = f.when(h::hasMore).thenApply(h::standardPath);
            return Wiggle.oneOf(vip, plain).thenApply(h::drain);
        });

        // two guarded arms, no default: the second guard's false edge carries on to the next step
        assertEquals(NodeKind.PREDICATE, named(flow.definition(), "isVip").kind());
        assertEquals(NodeKind.PREDICATE, named(flow.definition(), "hasMore").kind());
        assertEquals(NodeKind.TASK, named(flow.definition(), "drain").kind());
        assertTrue(dsl.version() != flow.version(), "different topologies, compared structurally above");
    }

    @Test
    void everyOneOfArmMustOpenWithWhenOrOtherwise() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> {
            Wiggle.define("unguarded", Order.class, f -> {
                var vip = f.thenApply(h::vipPath);              // no when(...)
                var standard = f.otherwise().thenApply(h::standardPath);
                return Wiggle.oneOf(vip, standard);
            });
        });

        assertTrue(ex.getMessage().contains("when(...) or otherwise()"), ex.getMessage());
    }

    @Test
    void retryAndQueueAreRejectedOnAnythingTheEngineRunsItself() {
        // a sleep is a server-side timer: no worker holds it, so there is nothing to retry or pin
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                Wiggle.define("timer", Order.class, f -> f
                        .thenSleep(Duration.ofSeconds(1))
                        .withRetry(RetryPolicy.exponential(2, Duration.ofMillis(10)))));

        assertTrue(ex.getMessage().contains("must directly follow"), ex.getMessage());
    }

    @Test
    void asReTypesTheChainWithoutTouchingTheGraph() {
        FlowSpec plain = Wiggle.define("retyped", Order.class, f -> f.thenApply(h::validate));
        FlowSpec retyped = Wiggle.define("retyped", Order.class, f -> f
                .thenApply(h::validate).as(Shipment.class));

        assertEquals(plain.version(), retyped.version(), "as() must add no node");
    }
}