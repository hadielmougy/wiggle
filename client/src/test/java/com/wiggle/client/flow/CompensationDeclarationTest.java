package com.wiggle.client.flow;

import com.wiggle.client.worker.Activity;
import com.wiggle.client.worker.CompensableActivity;
import com.wiggle.client.worker.Compensation;
import com.wiggle.core.Node;
import com.wiggle.core.WorkflowDefinition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A step is compensable because its <em>declaration</em> says so, and nowhere else.
 *
 * <p>The flag matters server-side: the engine reads {@code node.compensable()} when a step completes
 * and only then captures the input/result snapshots the reverse pass needs. The server has the graph
 * and nothing else -- no handler classes, possibly not even the same language -- so whatever decides
 * this has to be readable while the workflow is being defined. A zero-argument factory's return type
 * is: {@link CompensableActivity} means "this has an undo", a plain {@link Activity} does not.
 */
class CompensationDeclarationTest {

    record Order(String id) {}

    interface Steps {
        /** Declares an undo, so the node carries the flag. */
        CompensableActivity<Order> reserve();

        /** Same factory shape, no undo declared. */
        Activity<Order> audit();

        Order confirm(Order o);
    }

    private static Node named(WorkflowDefinition def, String name) {
        return def.nodes().values().stream().filter(n -> name.equals(n.name())).findFirst()
                .orElseThrow(() -> new AssertionError("no node named " + name));
    }

    @Test
    @DisplayName("a CompensableActivity return marks the node; a plain Activity does not")
    void theReturnTypeDecides() {
        WorkflowDefinition def = FlowSpec.define("undo-decl", Order.class, Steps.class, (f, s) -> f
                .thenActivity(s::reserve)
                .thenActivity(s::audit)
                .thenApply(s::confirm)).definition();

        assertTrue(named(def, "reserve").compensable(),
                "declared CompensableActivity -> the engine will capture snapshots for it");
        assertFalse(named(def, "audit").compensable(),
                "declared a plain Activity -> an ordinary step, nothing captured");
        assertFalse(named(def, "confirm").compensable(),
                "an ordinary step form is never compensable");
    }

    @Test
    @DisplayName("the flag rides the content hash, so declaring an undo is a new version")
    void theFlagIsPartOfTheVersion() {
        int withUndo = FlowSpec.define("undo-ver", Order.class, Steps.class,
                (f, s) -> f.thenActivity(s::reserve)).version();
        int without = FlowSpec.define("undo-ver", Order.class, Steps.class,
                (f, s) -> f.thenActivity(s::audit)).version();

        // Not merely "different": the two graphs differ only in the step's name and its flag, and a
        // definition's version is a hash over the whole topology -- so adding an undo to a live
        // workflow publishes a new version rather than changing what running instances do.
        assertTrue(withUndo != without, "an undo is part of the topology, so part of its identity");
    }

    /** Named for the shape it declares; nothing here runs, the worker supplies the code. */
    static final class Reserve implements CompensableActivity<Order> {
        @Override public Order execute(Order o) { return o; }
        @Override public void compensate(Compensation<Order> c) { }
    }
}
