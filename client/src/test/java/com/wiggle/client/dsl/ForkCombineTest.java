package com.wiggle.client.dsl;

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

/** {@link WorkflowStream#fork} with its mandatory {@link ForkStage#combine}: isolated branches
 *  rejoined by an explicit aggregator. This covers the graph shape the DSL emits; the isolation
 *  semantics are exercised end-to-end in the engine tests. */
class ForkCombineTest {

    /** air/hotel each contribute a price; combine sums them. */
    private static Blueprint tripBlueprint() {
        return Workflow.define("trip")
                .step("prep", ctx -> ctx)
                .fork(
                        Branch.of("air", s -> s.step("book-air", ctx -> Map.of("air", 100))),
                        Branch.of("hotel", s -> s.step("book-hotel", ctx -> Map.of("hotel", 75))))
                .combine("merge", (ctx, parts) -> Map.of("total",
                        price(parts.get("air"), "air") + price(parts.get("hotel"), "hotel")))
                .step("book", ctx -> ctx)
                .build();
    }

    @SuppressWarnings("unchecked")
    private static int price(Object armResult, String key) {
        return (int) ((Map<String, Object>) armResult).get(key);
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
    void forkCombineWiresForkBranchesJoinAggregator() {
        WorkflowDefinition def = tripBlueprint().definition();

        Node fork = only(def, NodeKind.FORK);
        Node join = only(def, NodeKind.JOIN);
        Node air = named(def, "book-air");
        Node hotel = named(def, "book-hotel");
        Node merge = named(def, "merge");
        Node book = named(def, "book");

        // fork -> the two branch starts
        assertEquals(List.of(air.id(), hotel.id()), fork.branches());
        assertNull(fork.next(), "a static FORK carries no next edge");

        // each branch -> JOIN
        assertEquals(join.id(), air.next());
        assertEquals(join.id(), hotel.next());

        // JOIN(expected=2) -> the mandatory combine -> book -> end
        assertEquals(2, join.expected());
        assertEquals(merge.id(), join.next());
        assertEquals(NodeKind.TASK, merge.kind());
        assertEquals(book.id(), merge.next());
        assertEquals(NodeKind.END, def.nodes().get(book.next()).kind());
    }

    @Test
    void combineNodeCarriesArmNamesSoTheEngineCanKeyBranchResults() {
        WorkflowDefinition def = tripBlueprint().definition();
        Node merge = named(def, "merge");
        // The arm names ride on the combine node's itemsKey (a store-portable field) as a JSON array,
        // in fork order, so the engine can key each isolated branch's result at the join.
        assertEquals("[\"air\",\"hotel\"]", merge.itemsKey());
    }

    @Test
    void combineReadsArmsByNameAndReturnsMergedFields() throws Exception {
        Blueprint bp = tripBlueprint();
        // The engine stages each isolated arm's result under its name; the handler reads them.
        Map<String, Object> staged = Map.of("air", Map.of("air", 100), "hotel", Map.of("hotel", 75));
        @SuppressWarnings("unchecked")
        Map<String, Object> delta = (Map<String, Object>) bp.handlers().get("trip#merge").invoke(staged);
        assertEquals(175, delta.get("total"));
    }

    @Test
    void combineIsMandatory_forgottenCombineFailsBuild() {
        WorkflowStream stream = Workflow.define("t")
                .step("prep", ctx -> ctx);
        // Fan out but never combine: the fork is left pending.
        stream.fork(
                Branch.of("a", s -> s.step("a", ctx -> Map.of())),
                Branch.of("b", s -> s.step("b", ctx -> Map.of())));

        IllegalStateException ex = assertThrows(IllegalStateException.class, stream::build);
        assertTrue(ex.getMessage().toLowerCase().contains("merge")
                || ex.getMessage().toLowerCase().contains("combine"), ex.getMessage());
    }

    @Test
    void combineTwiceThrows() {
        WorkflowStream stream = Workflow.define("t")
                .step("prep", ctx -> ctx);
        ForkStage stage = stream.fork(
                Branch.of("a", s -> s.step("a", ctx -> Map.of())),
                Branch.of("b", s -> s.step("b", ctx -> Map.of())));
        stage.combine("m", (ctx, parts) -> Map.of());
        assertThrows(IllegalStateException.class, () -> stage.combine("m2", (ctx, parts) -> Map.of()));
    }

    @Test
    void combineResumesNormalFlowAndBuilds() {
        Blueprint bp = tripBlueprint();
        assertTrue(bp.definition().version() != 0);
        WorkflowDefinition def = bp.definition();
        assertEquals(NodeKind.END, def.nodes().get(named(def, "book").next()).kind());
    }
}