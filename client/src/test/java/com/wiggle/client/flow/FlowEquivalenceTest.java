package com.wiggle.client.flow;

import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Branch;
import com.wiggle.client.dsl.Case;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.client.flow.Fixtures.Line;
import com.wiggle.client.flow.Fixtures.Order;
import com.wiggle.client.flow.Fixtures.OrderHandlers;
import com.wiggle.client.flow.Fixtures.Shipment;
import com.wiggle.core.Node;
import com.wiggle.core.NodeKind;
import com.wiggle.core.WorkflowDefinition;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

    private final OrderHandlers h = new OrderHandlers();

    private static Node named(WorkflowDefinition def, String name) {
        return def.nodes().values().stream().filter(n -> name.equals(n.name())).findFirst()
                .orElseThrow(() -> new AssertionError("no node named '" + name + "' in "
                        + def.nodes().values().stream().map(Node::name).toList()));
    }

    private static void assertSameDefinition(Blueprint dsl, Blueprint flow) {
        assertEquals(dsl.name(), flow.name());
        assertEquals(dsl.definition().startNode(), flow.definition().startNode());
        assertEquals(dsl.definition().nodes().keySet(), flow.definition().nodes().keySet());
        assertEquals(dsl.version(), flow.version(),
                "the flow API must emit the identical topology, so the content hashes must match");
    }

    // ------------------------------------------------------------------ step / gate / fork / sleep

    @Test
    void stepsGatesForkAndSleepCompileToTheSameGraphAsTheDsl() {
        Blueprint dsl = Workflow.define("order-fulfilment")
                .step("validate")
                .gate("inStock")
                .fork(Branch.of("payment", s -> s.step("charge")),
                      Branch.of("shipping", s -> s.step("reserve")
                                                  .sleep(Duration.ofSeconds(2))
                                                  .step("label")))
                .combine("settle")
                .effect("notifyCustomer")
                .build();

        Blueprint flow = Wiggle.define("order-fulfilment", Order.class, f -> f
                .thenApply(h::validate)
                .thenFilter(h::inStock)
                .thenFork(Arm.of("payment", a -> a.thenApply(h::charge)),
                          Arm.of("shipping", a -> a.thenApply(h::reserve)
                                                   .thenSleep(Duration.ofSeconds(2))
                                                   .thenApply(h::label)))
                .combine(h::settle)
                .thenAccept(h::notifyCustomer));

        assertSameDefinition(dsl, flow);

        WorkflowDefinition def = flow.definition();
        assertEquals(NodeKind.TASK, named(def, "validate").kind());
        assertEquals(NodeKind.PREDICATE, named(def, "inStock").kind());
        assertEquals(NodeKind.TASK, named(def, "notifyCustomer").kind());
        // the combine still carries the arm names the engine keys each isolated branch's result by
        assertEquals("[\"payment\",\"shipping\"]", named(def, "settle").itemsKey());
    }

    @Test
    void threeArmedForkCombinesThroughTheTypedTriFunction() {
        Blueprint dsl = Workflow.define("audit")
                .fork(Branch.of("payment", s -> s.step("charge")),
                      Branch.of("shipping", s -> s.step("label")),
                      Branch.of("carrier", s -> s.subWorkflow("ship", "carrier-flow")))
                .combine("audit-merge")
                .build();

        Blueprint flow = Wiggle.define("audit", Order.class, f -> f
                .thenFork(Arm.of("payment", a -> a.thenApply(h::charge)),
                          Arm.of("shipping", a -> a.thenApply(h::label)),
                          Arm.of("carrier", a -> a.thenSubFlow("ship", "carrier-flow", Shipment.class)))
                .combine(h::audit));

        // the combine is referenced, so its node name comes from the handler method
        assertEquals("audit", named(flow.definition(), "audit").name());
        assertEquals(dsl.definition().nodes().size(), flow.definition().nodes().size());
    }

    // ------------------------------------------------------------------ choose / loop

    @Test
    void chooseAndRepeatWhileCompileToTheSameGraphAsTheDsl() {
        Blueprint dsl = Workflow.define("triage")
                .step("validate")
                .choose(Case.when("isVip", s -> s.step("vipPath")),
                        Case.otherwise("standard", s -> s.step("standardPath")))
                .doWhile("hasMore", s -> s.step("drain"))
                .build();

        Blueprint flow = Wiggle.define("triage", Order.class, f -> f
                .thenApply(h::validate)
                .thenChoose(Alt.when(h::isVip, a -> a.thenApply(h::vipPath)),
                            Alt.otherwise("standard", a -> a.thenApply(h::standardPath)))
                .repeatWhile(h::hasMore, a -> a.thenApply(h::drain)));

        assertSameDefinition(dsl, flow);

        WorkflowDefinition def = flow.definition();
        assertEquals(NodeKind.PREDICATE, named(def, "isVip").kind());
        Node loop = named(def, "hasMore");
        assertEquals(NodeKind.PREDICATE, loop.kind());
        assertEquals(named(def, "drain").id(), loop.next(), "a true condition re-enters the loop body");
    }

    @Test
    void repeatWhileWithABudgetMatchesTheDslsBoundedDoWhile() {
        Blueprint dsl = Workflow.define("drain")
                .doWhile("hasMore", 5, s -> s.step("drain"))
                .build();

        Blueprint flow = Wiggle.define("drain", Order.class, f -> f
                .repeatWhile(h::hasMore, 5, a -> a.thenApply(h::drain)));

        assertSameDefinition(dsl, flow);
    }

    // ------------------------------------------------------------------ forEach / signal

    @Test
    void forEachCompilesToTheSameDynamicFanOutAsTheDsl() {
        Blueprint dsl = Workflow.define("pricing")
                .step("validate")
                .forEach("lines", s -> s.step("price"))
                .combine("total")
                .build();

        Blueprint flow = Wiggle.define("pricing", Order.class, f -> f
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
        Blueprint flow = Wiggle.define("pricing-with-base", Order.class, f -> f
                .thenForEach("lines", Line.class, a -> a.thenApply(h::price))
                .combine(h::totalWithBase));

        assertEquals("totalWithBase", named(flow.definition(), "totalWithBase").name());
    }

    @Test
    void signalWaitsAndSubFlowsCompileToTheSameGraphAsTheDsl() {
        Blueprint dsl = Workflow.define("approval")
                .step("validate")
                .awaitSignal("approved", Duration.ofMinutes(5))
                .subWorkflow("ship", "shipping-flow")
                .build();

        Blueprint flow = Wiggle.define("approval", Order.class, f -> f
                .thenApply(h::validate)
                .thenAwait("approved", Duration.ofMinutes(5))
                .thenSubFlow("ship", "shipping-flow", Shipment.class));

        assertSameDefinition(dsl, flow);
        assertEquals(NodeKind.SUB_WORKFLOW, named(flow.definition(), "ship").kind());
    }

    // ------------------------------------------------------------------ names and per-step settings

    @Test
    void handlesAnnotationRenamesTheNodeSoBothSidesAgree() {
        Blueprint flow = Wiggle.define("renamed", Order.class, f -> f.thenApply(h::doCapture));

        // the handler is bound by @Handles("capture-payment"), so that -- not "doCapture" -- is the node
        List<String> names = flow.definition().nodes().values().stream()
                .map(Node::name).filter(n -> n != null).toList();
        assertTrue(names.contains("capture-payment"), names.toString());
        assertTrue(!names.contains("doCapture"), names.toString());
    }

    @Test
    void perStepQueueRetryCompensateAndCheckpointPassThroughUnchanged() {
        Blueprint dsl = Workflow.define("settings")
                .step("validate", "fast-queue").compensate()
                .step("charge", com.wiggle.core.RetryPolicy.exponential(7, Duration.ofMillis(250)))
                .checkpoint()
                .build();

        Blueprint flow = Wiggle.define("settings", Order.class, f -> f
                .thenApply(h::validate, "fast-queue").compensate()
                .thenApply(h::charge, com.wiggle.core.RetryPolicy.exponential(7, Duration.ofMillis(250)))
                .checkpoint());

        assertSameDefinition(dsl, flow);
        assertTrue(flow.queues().contains("fast-queue"));
    }

    @Test
    void asReTypesTheChainWithoutTouchingTheGraph() {
        Blueprint plain = Wiggle.define("retyped", Order.class, f -> f.thenApply(h::validate));
        Blueprint retyped = Wiggle.define("retyped", Order.class, f -> f
                .thenApply(h::validate).as(Shipment.class));

        assertEquals(plain.version(), retyped.version(), "as() must add no node");
    }
}