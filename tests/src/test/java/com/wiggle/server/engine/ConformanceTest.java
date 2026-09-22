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
}
