package com.wiggle.server.engine;

import com.wiggle.core.ExecutionMode;
import com.wiggle.core.Node;
import com.wiggle.core.WorkflowDefinition;
import com.wiggle.server.engine.Conformance.Finding;
import com.wiggle.server.engine.Conformance.Step;
import com.wiggle.server.engine.Conformance.Verdict;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/** The judge as a pure function: a graph, steps in time order, a verdict. */
class ConformanceTest {

    /** a -> b -> keep? -> c -> ok ; keep=false -> dropped(END, failed). */
    private static WorkflowDefinition linear() {
        Map<String, Node> n = new LinkedHashMap<>();
        n.put("a", Node.task("a", "a", "act", "q", null).withNext("b"));
        n.put("b", Node.task("b", "b", "act", "q", null).withNext("keep"));
        n.put("keep", Node.predicate("keep", "keep", "act", "q", null).withNext("c").withAltNext("dropped"));
        n.put("c", Node.task("c", "c", "act", "q", null).withNext("ok"));
        n.put("ok", Node.end("ok", true, "done"));
        n.put("dropped", Node.end("dropped", false, "filtered"));
        return new WorkflowDefinition("lin", 1, "a", n, Set.of("q"), ExecutionMode.OBSERVED);
    }

    /** a -> fork(x, y) -> join -> z -> ok. */
    private static WorkflowDefinition forked() {
        Map<String, Node> n = new LinkedHashMap<>();
        n.put("a", Node.task("a", "a", "act", "q", null).withNext("f"));
        n.put("f", Node.fork("f", "f").withBranches(List.of("x", "y")));
        n.put("x", Node.task("x", "x", "act", "q", null).withNext("j"));
        n.put("y", Node.task("y", "y", "act", "q", null).withNext("j"));
        n.put("j", Node.join("j", "j", 2).withNext("z"));
        n.put("z", Node.task("z", "z", "act", "q", null).withNext("ok"));
        n.put("ok", Node.end("ok", true, "done"));
        return new WorkflowDefinition("fork", 1, "a", n, Set.of("q"), ExecutionMode.OBSERVED);
    }

    /** a -> again? -> (true) a ... (false) ok: a loop where a legitimately repeats. */
    private static WorkflowDefinition looped() {
        Map<String, Node> n = new LinkedHashMap<>();
        n.put("a", Node.task("a", "a", "act", "q", null).withNext("again"));
        n.put("again", Node.predicate("again", "again", "act", "q", null).withNext("a").withAltNext("ok"));
        n.put("ok", Node.end("ok", true, "done"));
        return new WorkflowDefinition("loop", 1, "a", n, Set.of("q"), ExecutionMode.OBSERVED);
    }

    /** reserve(undo) -> charge(undo) -> ship -> ok: a saga. */
    private static WorkflowDefinition saga() {
        Map<String, Node> n = new LinkedHashMap<>();
        n.put("reserve", Node.task("reserve", "reserve", "act", "q", null).withNext("charge").withCompensable());
        n.put("charge", Node.task("charge", "charge", "act", "q", null).withNext("ship").withCompensable());
        n.put("ship", Node.task("ship", "ship", "act", "q", null).withNext("ok"));
        n.put("ok", Node.end("ok", true, "done"));
        return new WorkflowDefinition("saga", 1, "reserve", n, Set.of("q"), ExecutionMode.OBSERVED);
    }

    private static Step undo(String node, long t) { return Step.undo(node, t, 0, false); }
    private static Step undoFailed(String node, long t) { return Step.undo(node, t, 0, true); }
    private static Step threw(String node, long t) { return new Step(node, null, t, 0, null, null, true); }

    private static Step at(String node, long t) { return new Step(node, null, t); }
    private static Step pred(String node, boolean v, long t) { return new Step(node, v, t); }
    private static List<String> kinds(Verdict v) { return v.findings().stream().map(Finding::kind).toList(); }

    @Test @DisplayName("a run that follows the graph completes with no findings")
    void cleanRun() {
        Verdict v = Conformance.judge(linear(), List.of(at("a", 1), at("b", 2), pred("keep", true, 3), at("c", 4)));
        assertTrue(v.completed());
        assertEquals(List.of(), v.findings());
    }

    @Test @DisplayName("a predicate's value picks its branch; a failing END fails with its reason")
    void predicateBranch() {
        Verdict v = Conformance.judge(linear(), List.of(at("a", 1), at("b", 2), pred("keep", false, 3)));
        assertFalse(v.completed());
        assertEquals("filtered", v.endReason());
        assertEquals(List.of(), v.findings(), "the false branch is in the topology");
    }

    @Test @DisplayName("interleaved branches of a fork are all expected; the join releases after both")
    void forkBranchesInterleave() {
        Verdict v = Conformance.judge(forked(), List.of(at("a", 1), at("y", 2), at("x", 3), at("z", 4)));
        assertTrue(v.completed());
        assertEquals(List.of(), v.findings());
    }

    @Test @DisplayName("a step after the join before both branches arrived is out of order")
    void joinTooEarly() {
        Verdict v = Conformance.judge(forked(), List.of(at("a", 1), at("x", 2), at("z", 3), at("y", 4)));
        assertEquals(List.of("OUT_OF_ORDER", "OUT_OF_ORDER"), kinds(v), v.findings().toString());
        assertEquals("y", v.findings().getFirst().expected(), "y was still due when z came");
        assertEquals("z", v.findings().getFirst().reported());
        assertTrue(v.completed(), "z's successor was END; the late y is recorded, not fatal");
    }

    @Test @DisplayName("a step reported twice outside a loop is a duplicate, ignored")
    void duplicate() {
        Verdict v = Conformance.judge(linear(), List.of(at("a", 1), at("b", 2), at("b", 3), pred("keep", true, 4), at("c", 5)));
        assertEquals(List.of("DUPLICATE"), kinds(v));
        assertEquals("b", v.findings().getFirst().reported());
        assertEquals("keep", v.findings().getFirst().expected());
        assertTrue(v.completed());
    }

    @Test @DisplayName("a step inside a declared loop may repeat without a finding")
    void loopRepeats() {
        Verdict v = Conformance.judge(looped(), List.of(at("a", 1), pred("again", true, 2), at("a", 3),
                pred("again", true, 4), at("a", 5), pred("again", false, 6)));
        assertTrue(v.completed());
        assertEquals(List.of(), v.findings());
    }

    @Test @DisplayName("a step out of order resynchronises the frontier so the rest still judges")
    void outOfOrderResyncs() {
        Verdict v = Conformance.judge(linear(), List.of(at("a", 1), at("c", 2)));
        assertEquals(List.of("OUT_OF_ORDER"), kinds(v));
        assertEquals("b", v.findings().getFirst().expected());
        assertEquals("c", v.findings().getFirst().reported());
        assertTrue(v.completed(), "c's successor is END");
    }

    @Test @DisplayName("a run that never reaches END is incomplete, naming where it stopped")
    void incomplete() {
        Verdict v = Conformance.judge(linear(), List.of(at("a", 1), at("b", 2)));
        assertEquals(List.of("INCOMPLETE"), kinds(v));
        assertFalse(v.reachedEnd());
        assertEquals("keep", v.stoppedAt());
        assertEquals("keep", v.findings().getFirst().expected());
    }

    @Test @DisplayName("an empty run is incomplete at the start node")
    void emptyRun() {
        Verdict v = Conformance.judge(linear(), List.of());
        assertEquals(List.of("INCOMPLETE"), kinds(v));
        assertEquals("a", v.stoppedAt());
    }

    @Test @DisplayName("unknown steps are the arrival path's business: the judge skips them")
    void unknownSkipped() {
        Verdict v = Conformance.judge(linear(), List.of(at("a", 1), at("nope", 2), at("b", 3), pred("keep", true, 4), at("c", 5)));
        assertTrue(v.completed());
        assertEquals(List.of(), v.findings());
    }

    @Test @DisplayName("cycle detection: only nodes that can reach themselves")
    void cycles() {
        assertEquals(Set.of("a", "again"), Conformance.cyclicNodes(looped()));
        assertEquals(Set.of(), Conformance.cyclicNodes(linear()));
        assertEquals(Set.of(), Conformance.cyclicNodes(forked()));
    }

    private static Step after(String node, long t, long seq, String cause) { return new Step(node, null, t, seq, cause); }

    @Test @DisplayName("a causal hint orders a receiver's early-clocked step after its cause")
    void hintBeatsSkewedClock() {
        // service B's clock runs 50ms early: c is stamped before b, but names b as its cause
        List<Step> steps = List.of(at("a", 100), at("b", 200), pred("keep", true, 210), after("c", 160, 9, "keep"));
        List<String> unhinted = kinds(Conformance.judge(linear(), List.of(at("a", 100), at("b", 200),
                pred("keep", true, 210), at("c", 160))));
        assertFalse(unhinted.isEmpty(), "without the hint the clock misleads");
        assertTrue(unhinted.stream().allMatch("OUT_OF_ORDER"::equals), unhinted.toString());
        Verdict v = Conformance.judge(linear(), steps);
        assertTrue(v.completed());
        assertEquals(List.of(), v.findings(), "with the hint, c follows keep whatever its clock said");
    }

    @Test @DisplayName("a hint the graph disagrees with is ignored: a sibling branch is no cause")
    void hintMustBeAPredecessor() {
        // x names y as its cause; they are sibling fork branches, so the clock stays in charge
        Verdict v = Conformance.judge(forked(), List.of(at("a", 1), after("x", 2, 1, "y"), at("y", 3), at("z", 4)));
        assertTrue(v.completed());
        assertEquals(List.of(), v.findings());
        assertEquals(List.of("a", "x", "y", "z"), Conformance.order(forked(),
                List.of(at("a", 1), after("x", 2, 1, "y"), at("y", 3), at("z", 4))).stream().map(Step::nodeId).toList());
    }

    @Test @DisplayName("a hint to a step never reported falls back to the clock rather than blocking")
    void hintToMissingCauseFallsBack() {
        Verdict v = Conformance.judge(linear(), List.of(at("a", 1), after("c", 2, 1, "keep")));
        assertEquals(List.of("OUT_OF_ORDER"), kinds(v), "b and keep are missing; c is judged where its clock puts it");
        assertTrue(v.completed());
    }

    @Test @DisplayName("a hint names a node, not an occurrence: it resolves to the latest one by clock, or the next when clocks skew")
    void hintResolvesByClock() {
        // clean loop: the hint names an occurrence already before the step; nothing moves
        List<Step> clean = List.of(at("a", 1), pred("again", true, 2), after("a", 5, 1, "again"), pred("again", false, 6));
        assertEquals(List.of("a", "again", "a", "again"), Conformance.order(looped(), clean).stream().map(Step::nodeId).toList());
        assertTrue(Conformance.judge(looped(), clean).completed());
        // skewed: the second 'a' is clocked before the 'again' that caused it; the hint pins it after
        List<Step> skewed = List.of(at("a", 1), after("a", 4, 1, "again"), pred("again", true, 6), pred("again", false, 8));
        assertEquals(List.of("a", "again", "a", "again"), Conformance.order(looped(), skewed).stream().map(Step::nodeId).toList());
        assertEquals(List.of(), Conformance.judge(looped(), skewed).findings());
    }

    @Test @DisplayName("predecessors: what a hint may legitimately name")
    void predecessors() {
        Map<String, Set<String>> p = Conformance.predecessors(forked());
        assertEquals(Set.of("a"), p.get("x"));
        assertEquals(Set.of("a"), p.get("y"));
        assertEquals(Set.of("a", "x", "y"), p.get("z"));
        assertNull(p.get("a"), "the start has no predecessor");
        assertEquals(Set.of("a", "b", "keep"), Conformance.predecessors(linear()).get("c"));
        assertEquals(Set.of("a", "again"), Conformance.predecessors(looped()).get("a"), "a loop makes a step its own predecessor");
    }

    @Test @DisplayName("after a failure, every completed compensable step undone newest-first is a clean compensation")
    void compensatedCleanly() {
        Verdict v = Conformance.judge(saga(), List.of(at("reserve", 1), at("charge", 2), threw("ship", 3),
                undo("charge", 4), undo("reserve", 5)), true);
        assertTrue(v.compensated());
        assertNull(v.compensationError());
        assertEquals(List.of(), v.findings(), "no INCOMPLETE for a failed run, no ordering finding");
        assertFalse(v.completed());
    }

    @Test @DisplayName("an undo still missing when judged fails the compensation and names it")
    void missingUndo() {
        Verdict v = Conformance.judge(saga(), List.of(at("reserve", 1), at("charge", 2), undo("charge", 3)), true);
        assertFalse(v.compensated());
        assertEquals("undo of reserve never reported", v.compensationError());
        assertEquals(List.of("MISSING_UNDO"), kinds(v));
        assertEquals("reserve", v.findings().getFirst().expected());
    }

    @Test @DisplayName("undos out of order are recorded but still count; a failed undo fails the compensation")
    void undoOrderAndFailure() {
        Verdict v = Conformance.judge(saga(), List.of(at("reserve", 1), at("charge", 2),
                undo("reserve", 3), undo("charge", 4)), true);
        assertTrue(v.compensated(), "both undos arrived");
        assertEquals(List.of("UNDO_OUT_OF_ORDER"), kinds(v));
        assertEquals("charge", v.findings().getFirst().expected(), "charge was due first, newest first");

        Verdict failed = Conformance.judge(saga(), List.of(at("reserve", 1), at("charge", 2),
                undoFailed("charge", 3), undo("reserve", 4)), true);
        assertFalse(failed.compensated());
        assertEquals("undo of charge failed", failed.compensationError());
    }

    @Test @DisplayName("an undo the run never earned, or one without a failure, is a finding")
    void undoWithoutStepOrFailure() {
        Verdict v = Conformance.judge(saga(), List.of(at("reserve", 1), undo("charge", 2), undo("reserve", 3)), true);
        assertEquals(List.of("UNDO_WITHOUT_STEP"), kinds(v), "charge never completed");
        assertTrue(v.compensated(), "reserve, the one step completed, was undone");

        Verdict noFailure = Conformance.judge(saga(), List.of(at("reserve", 1), at("charge", 2), at("ship", 3), undo("charge", 4)), false);
        assertTrue(noFailure.completed(), "the run reached END regardless");
        assertEquals(List.of("UNDO_WITHOUT_FAILURE"), kinds(noFailure));
        assertFalse(noFailure.compensated(), "nothing was owed");
    }

    @Test @DisplayName("a failure with nothing completed to undo compensates vacuously; a repeated undo is a duplicate")
    void nothingToUndoAndDuplicateUndo() {
        Verdict v = Conformance.judge(saga(), List.of(threw("reserve", 1)), true);
        assertTrue(v.compensated());
        assertEquals(List.of(), v.findings());

        Verdict dup = Conformance.judge(saga(), List.of(at("reserve", 1), threw("charge", 2), undo("reserve", 3), undo("reserve", 4)), true);
        assertTrue(dup.compensated());
        assertEquals(List.of("DUPLICATE"), kinds(dup));
    }
}
