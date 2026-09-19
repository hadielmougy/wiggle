package com.wiggle.client.flow;

import com.wiggle.core.Node;
import com.wiggle.core.NodeKind;
import com.wiggle.core.WorkflowDefinition;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link Wiggle#allOf} with its mandatory combine: the topology it emits -- an isolated fork rejoined
 * by a combine node that carries the arm names. The combine's merge logic is a worker concern,
 * exercised end-to-end in the engine tests.
 */
class ForkCombineTest {

    interface TripSteps {
        Map<String, Object> prep(Map<String, Object> ctx);
        Map<String, Object> bookAir(Map<String, Object> ctx);
        Map<String, Object> bookHotel(Map<String, Object> ctx);
        Map<String, Object> merge(Map<String, Object> air, Map<String, Object> hotel);
        Map<String, Object> book(Map<String, Object> ctx);
    }

    private static FlowSpec tripFlowSpec() {
        return FlowSpec.define("trip", 1, Map.class, TripSteps.class, (f, s) -> {
            var prepped = f.thenApply(s::prep);
            // continuing `prepped` twice is the fan-out; each arm runs on its own isolated copy
            var air = prepped.thenApply(s::bookAir);
            var hotel = prepped.thenApply(s::bookHotel);
            return Wiggle.allOf(air, hotel).combine(s::merge).thenApply(s::book);
        });
    }

    private static Node only(WorkflowDefinition def, NodeKind kind) {
        List<Node> hits = def.nodes().values().stream().filter(n -> n.kind() == kind).toList();
        assertEquals(1, hits.size(), "expected exactly one " + kind + " node");
        return hits.get(0);
    }

    private static Node named(WorkflowDefinition def, String name) {
        return def.nodes().values().stream().filter(n -> name.equals(n.name())).findFirst()
                .orElseThrow(() -> new AssertionError("no node named " + name));
    }

    @Test
    void forkCombineWiresForkBranchesJoinCombine() {
        WorkflowDefinition def = tripFlowSpec().definition();

        Node fork = only(def, NodeKind.FORK);
        Node join = only(def, NodeKind.JOIN);
        Node air = named(def, "bookAir");
        Node hotel = named(def, "bookHotel");
        Node merge = named(def, "merge");
        Node book = named(def, "book");

        assertEquals(List.of(air.id(), hotel.id()), fork.branches());
        assertNull(fork.next(), "a static FORK carries no next edge");
        assertEquals(join.id(), air.next());
        assertEquals(join.id(), hotel.next());
        assertEquals(2, join.expected());
        assertEquals(merge.id(), join.next());
        assertEquals(NodeKind.TASK, merge.kind());
        assertEquals(book.id(), merge.next());
        assertEquals(NodeKind.END, def.nodes().get(book.next()).kind());
    }

    @Test
    void combineNodeCarriesArmNamesForTheEngineToKeyBranchResults() {
        WorkflowDefinition def = tripFlowSpec().definition();
        // The arm names ride on the combine node in fork order, so the engine can stage each
        // isolated branch's result under its name. The arm names are the step names, which in this
        // API are the referenced methods' own names.
        assertEquals(java.util.List.of("bookAir", "bookHotel"), named(def, "merge").armNames());
        assertTrue(named(def, "merge").isCombine());
    }

    @Test
    void combineIsMandatory_aFanOutWithNoMergeFailsToDefine() {
        // Forgetting the combine entirely cannot compile -- allOf returns a stage whose only methods
        // are combines -- so what is reachable is dropping the stage on the floor, which leaves the
        // fan-out unjoined.
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                FlowSpec.define("t", 1, Map.class, TripSteps.class, (f, s) -> {
                    var prepped = f.thenApply(s::prep);
                    Wiggle.allOf(prepped.thenApply(s::bookAir), prepped.thenApply(s::bookHotel));
                    return prepped;
                }));
        assertTrue(ex.getMessage().toLowerCase().contains("merge")
                || ex.getMessage().toLowerCase().contains("combine"), ex.getMessage());
    }

    @Test
    void combineTwiceThrows() {
        assertThrows(IllegalStateException.class, () ->
                FlowSpec.define("t", 1, Map.class, TripSteps.class, (f, s) -> {
                    var prepped = f.thenApply(s::prep);
                    var stage = Wiggle.allOf(prepped.thenApply(s::bookAir), prepped.thenApply(s::bookHotel));
                    stage.combine(s::merge);
                    return stage.combine(s::merge);   // a fan-out joins once
                }));
    }

    @Test
    void combineResumesNormalFlowAndBuilds() {
        WorkflowDefinition def = tripFlowSpec().definition();
        assertTrue(def.version() != 0);
        assertEquals(NodeKind.END, def.nodes().get(named(def, "book").next()).kind());
    }
}
