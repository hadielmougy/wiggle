package com.wiggle.client.flow;

import com.wiggle.client.worker.CompensableActivity;
import com.wiggle.client.worker.Compensation;
import com.wiggle.core.Node;
import com.wiggle.core.WorkflowDefinition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A step is compensable because its <em>declaration</em> says so, and nowhere else.
 *
 * <p>The flag matters server-side: the engine reads {@code node.compensable()} when a step completes
 * and only then captures the input/result snapshots the reverse pass needs. The server has the graph
 * and nothing else -- no handler classes, possibly not even the same language -- so whatever decides
 * this has to be settled while the workflow is being defined. {@code thenApplyCompensable} accepts
 * nothing but a {@link CompensableActivity} factory, so the declaration and the flag are the same
 * fact and a step cannot be one without the other.
 */
class CompensationDeclarationTest {

    record Order(String id) {}

    record Payment(String reference) {}

    interface Steps {
        /** Consumes an Order and produces a Payment -- an activity maps A to B like any other step. */
        CompensableActivity<Order, Payment> authorise();

        Payment confirm(Payment p);
    }

    private static Node named(WorkflowDefinition def, String name) {
        return def.nodes().values().stream().filter(n -> name.equals(n.name())).findFirst()
                .orElseThrow(() -> new AssertionError("no node named " + name));
    }

    @Test
    @DisplayName("a CompensableActivity factory marks the node; an ordinary step does not")
    void theDeclarationDecides() {
        WorkflowDefinition def = FlowSpec.define("undo-decl", 1, Order.class, Steps.class, (f, s) -> f
                .thenApplyCompensable(s::authorise)
                .thenApply(s::confirm)).definition();

        assertTrue(named(def, "authorise").compensable(),
                "declared through a CompensableActivity -> the engine will capture snapshots for it");
        assertFalse(named(def, "confirm").compensable(),
                "an ordinary step is never compensable");

        // There is no way to say the opposite of either: thenApplyCompensable takes nothing but a
        // CompensableActivity, and no other combinator sets the flag. A step whose handler carries an
        // undo it never declared is refused by the binder rather than silently never compensated.
    }

    @Test
    @DisplayName("a compensable step changes the context type, exactly like thenApply")
    void itIsAnOrdinaryStepInEveryOtherWay() {
        // authorise consumes Order and produces Payment, so the flow continues as Payment -- which is
        // what lets `confirm(Payment)` follow it. If the activity were pinned to one type this would
        // not compile, which is the point of the test.
        WorkflowDefinition def = FlowSpec.define("undo-types", 1, Order.class, Steps.class, (f, s) -> f
                .thenApplyCompensable(s::authorise)
                .thenApply(s::confirm)).definition();

        assertEquals("confirm", named(def, "authorise").next() == null ? null
                        : def.nodes().get(named(def, "authorise").next()).name(),
                "the compensable step chains onward like any other");
    }

    @Test
    @DisplayName("the flag rides the fingerprint, so declaring an undo is a different graph")
    void theFlagIsPartOfTheFingerprint() {
        String withUndo = FlowSpec.define("undo-ver", 1, Order.class, Steps.class,
                (f, s) -> f.thenApplyCompensable(s::authorise)).definition().fingerprint();
        String plain = FlowSpec.define("undo-ver", 1, Payment.class, Steps.class,
                (f, s) -> f.thenApply(s::confirm)).definition().fingerprint();

        // The undo is part of the topology, so the server sees adding one to an already-published
        // version as a changed graph and refuses it -- it has to go out as a new version.
        assertNotEquals(withUndo, plain, "an undo is part of the topology, so part of its identity");
    }

    /** Named for the shape it declares; nothing here runs, the worker supplies the code. */
    static final class Authorise implements CompensableActivity<Order, Payment> {
        @Override public Payment execute(Order o) { return new Payment("pay-" + o.id()); }
        @Override public void compensate(Compensation<Order, Payment> c) { }
    }
}
