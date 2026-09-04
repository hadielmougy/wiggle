package com.wiggle.client.dsl;

import com.wiggle.core.Node;
import com.wiggle.core.NodeKind;
import com.wiggle.core.WorkflowDefinition;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** {@link WorkflowBuilder#fork} with its mandatory {@link ForkStage#combine}: the topology it emits
 *  (an isolated fork rejoined by a combine node that carries the arm names). The combine's merge
 *  logic is a worker concern, exercised end-to-end in the engine tests. */
class ForkCombineTest {

    private static Blueprint tripBlueprint() {
        return Workflow.define("trip")
                .step("prep")
                .fork(
                        Branch.of("air", s -> s.step("book-air")),
                        Branch.of("hotel", s -> s.step("book-hotel")))
                .combine("merge")
                .step("book")
                .build();
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
        WorkflowDefinition def = tripBlueprint().definition();

        Node fork = only(def, NodeKind.FORK);
        Node join = only(def, NodeKind.JOIN);
        Node air = named(def, "book-air");
        Node hotel = named(def, "book-hotel");
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
        WorkflowDefinition def = tripBlueprint().definition();
        // The arm names ride on the combine node's itemsKey (a store-portable field) as a JSON array,
        // in fork order, so the engine can stage each isolated branch's result under its name.
        assertEquals("[\"air\",\"hotel\"]", named(def, "merge").itemsKey());
    }

    @Test
    void combineIsMandatory_forgottenCombineFailsBuild() {
        WorkflowBuilder stream = Workflow.define("t").step("prep");
        stream.fork(Branch.of("a", s -> s.step("a")), Branch.of("b", s -> s.step("b")));

        IllegalStateException ex = assertThrows(IllegalStateException.class, stream::build);
        assertTrue(ex.getMessage().toLowerCase().contains("merge")
                || ex.getMessage().toLowerCase().contains("combine"), ex.getMessage());
    }

    @Test
    void combineTwiceThrows() {
        WorkflowBuilder stream = Workflow.define("t").step("prep");
        ForkStage stage = stream.fork(Branch.of("a", s -> s.step("a")), Branch.of("b", s -> s.step("b")));
        stage.combine("m");
        assertThrows(IllegalStateException.class, () -> stage.combine("m2"));
    }

    @Test
    void combineResumesNormalFlowAndBuilds() {
        WorkflowDefinition def = tripBlueprint().definition();
        assertTrue(def.version() != 0);
        assertEquals(NodeKind.END, def.nodes().get(named(def, "book").next()).kind());
    }
}
