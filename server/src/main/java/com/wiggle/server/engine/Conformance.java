package com.wiggle.server.engine;

import com.wiggle.core.Node;
import com.wiggle.core.NodeKind;
import com.wiggle.core.WorkflowDefinition;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Judges an observed run against its topology once the run has settled. Pure: the steps come in
 * sorted by the reporter's clock, and the verdict is a function of them and the graph alone, so
 * the same judgement can be replayed offline over any day of runs.
 *
 * <p>The judge walks a frontier, the set of steps the graph expects next. A step in the frontier
 * consumes it and releases its successors: a predicate's value picks the branch, a fork releases
 * every branch head, a join releases its successor once every branch has arrived, END closes the
 * run. Two branches whose steps interleave in time are both in the frontier, so fan-out across
 * services raises nothing. A step outside the frontier is a DUPLICATE when it was already
 * consumed and lies on no cycle (at-least-once delivery), and OUT_OF_ORDER otherwise, after which
 * the frontier resynchronises to that step's successors so the rest of the run still judges.
 */
final class Conformance {

    private Conformance() {}

    /** One reported step, in judging order; {@code failed} when the step threw. */
    record Step(String nodeId, Boolean predicateValue, long at, boolean failed) {

        Step(String nodeId, Boolean predicateValue, long at) {
            this(nodeId, predicateValue, at, false);
        }
    }

    record Finding(String kind, String expected, String reported, String detail) {}

    /**
     * The verdict: findings in order, whether a successful END was reached, the reason if an END
     * was reached that terminates the run as failed, and where the run stopped if it never got there.
     */
    record Verdict(List<Finding> findings, boolean completed, String endReason, String stoppedAt) {

        boolean reachedEnd() {
            return completed || endReason != null;
        }
    }

    static Verdict judge(WorkflowDefinition def, List<Step> steps) {
        Walk w = new Walk(def);
        for (Step s : steps) w.take(s);
        return w.verdict();
    }

    private static final class Walk {
        private final WorkflowDefinition def;
        private final Set<String> frontier = new LinkedHashSet<>();
        private final Set<String> consumed = new HashSet<>();
        private final Map<String, Integer> joinPending = new HashMap<>();
        private final Set<String> cyclic;
        private final List<Finding> findings = new ArrayList<>();
        private boolean completed;
        private String endReason;

        Walk(WorkflowDefinition def) {
            this.def = def;
            this.cyclic = cyclicNodes(def);
            frontier.add(def.startNode());
        }

        void take(Step s) {
            Node node = def.nodes().get(s.nodeId());
            if (node == null || !node.isWorkerDispatched()) return;   // recorded at arrival as UNKNOWN_NODE
            if (frontier.remove(node.id())) {
                consumed.add(node.id());
                if (!s.failed()) release(successorOf(node, s.predicateValue()));   // a step that threw leads nowhere
                return;
            }
            String expected = expectation();
            if (consumed.contains(node.id()) && !cyclic.contains(node.id())) {
                findings.add(new Finding(ObservedRunningMode.DUPLICATE, expected, node.id(),
                        node.name() + " ran again; the topology runs it once"));
                return;
            }
            findings.add(new Finding(ObservedRunningMode.OUT_OF_ORDER, expected, node.id(),
                    "expected " + expectedNames() + ", got " + node.name()));
            frontier.clear();
            consumed.add(node.id());
            if (!s.failed()) release(successorOf(node, s.predicateValue()));
        }

        Verdict verdict() {
            String stoppedAt = null;
            if (!completed && endReason == null) {
                stoppedAt = expectedNames();
                findings.add(new Finding(ObservedRunningMode.INCOMPLETE, expectation(), null,
                        "run ended before END, at " + stoppedAt));
            }
            return new Verdict(List.copyOf(findings), completed, endReason, stoppedAt);
        }

        private static String successorOf(Node node, Boolean predicateValue) {
            if (node.kind() == NodeKind.PREDICATE) {
                return predicateValue != null && predicateValue ? node.next() : node.altNext();
            }
            return node.next();
        }

        /** Adds what the graph expects after {@code id}, walking through the structural nodes. */
        private void release(String id) {
            Deque<String> work = new ArrayDeque<>();
            if (id != null) work.push(id);
            while (!work.isEmpty()) {
                Node n = def.nodes().get(work.pop());
                if (n == null) continue;
                switch (n.kind()) {
                    case TASK, PREDICATE -> frontier.add(n.id());
                    case FORK -> { for (String b : n.branches()) work.push(b); }
                    case JOIN -> {
                        int left = joinPending.getOrDefault(n.id(), Math.max(1, n.expected())) - 1;
                        if (left <= 0) { joinPending.remove(n.id()); if (n.next() != null) work.push(n.next()); }
                        else joinPending.put(n.id(), left);
                    }
                    case END -> {
                        if (n.success()) completed = true;
                        else endReason = n.reason() == null ? "terminated" : n.reason();
                    }
                    default -> { }   // never registered on an observed graph
                }
            }
        }

        private String expectation() {
            return frontier.isEmpty() ? "END" : String.join(",", frontier);
        }

        private String expectedNames() {
            if (frontier.isEmpty()) return "END";
            List<String> names = new ArrayList<>();
            for (String id : frontier) names.add(def.nodes().get(id).name());
            return String.join(", ", names);
        }
    }

    /** Nodes that can reach themselves: a step there may legitimately run more than once. */
    static Set<String> cyclicNodes(WorkflowDefinition def) {
        Set<String> out = new HashSet<>();
        for (Node n : def.nodes().values()) {
            if (!n.isWorkerDispatched()) continue;
            Set<String> seen = new HashSet<>();
            Deque<String> work = new ArrayDeque<>(successors(n));
            while (!work.isEmpty()) {
                String id = work.pop();
                if (id == null || !seen.add(id)) continue;
                if (id.equals(n.id())) { out.add(n.id()); break; }
                Node m = def.nodes().get(id);
                if (m != null) work.addAll(successors(m));
            }
        }
        return out;
    }

    private static List<String> successors(Node n) {
        List<String> out = new ArrayList<>();
        if (n.next() != null) out.add(n.next());
        if (n.altNext() != null) out.add(n.altNext());
        out.addAll(n.branches());
        return out;
    }
}
