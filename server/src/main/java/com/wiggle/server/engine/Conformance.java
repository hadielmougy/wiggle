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

    /**
     * One reported step. {@code at} is its clock and {@code seq} its arrival order, the priority
     * when nothing causal separates two steps; {@code after} is the node it named as its cause,
     * or null.
     */
    record Step(String nodeId, Boolean predicateValue, long at, long seq, String after) {

        Step(String nodeId, Boolean predicateValue, long at) {
            this(nodeId, predicateValue, at, 0, null);
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
        for (Step s : order(def, steps)) w.take(s);
        return w.verdict();
    }

    /**
     * The steps in judging order: by clock, then arrival, except where a step names its cause and
     * the graph agrees that cause precedes it -- then the cause goes first whatever the clocks
     * said. That is what lets two services' steps order correctly across a message boundary when
     * their clocks disagree.
     *
     * <p>A hint is honoured only when the named node is a predecessor of the step's node in the
     * topology; it resolves to the latest reported occurrence of that node up to the step's own
     * clock (or the earliest after it, when clocks are what is wrong). A hint that resolves to
     * nothing is ignored, so a lost report never blocks judgement, and a cycle -- which only a
     * bug can produce -- is broken by the clock.
     */
    static List<Step> order(WorkflowDefinition def, List<Step> steps) {
        List<Step> byClock = new ArrayList<>(steps);
        byClock.sort(java.util.Comparator.comparingLong(Step::at).thenComparingLong(Step::seq));
        if (byClock.stream().noneMatch(s -> s.after() != null)) return byClock;

        Map<String, Set<String>> before = predecessors(def);
        int n = byClock.size();
        List<List<Integer>> successors = new ArrayList<>(n);
        int[] unmet = new int[n];
        for (int i = 0; i < n; i++) successors.add(new ArrayList<>());
        for (int i = 0; i < n; i++) {
            Step s = byClock.get(i);
            if (s.after() == null || s.after().equals(s.nodeId())) continue;
            Set<String> preds = before.get(s.nodeId());
            if (preds == null || !preds.contains(s.after())) continue;
            int cause = resolveCause(byClock, i, s.after());
            if (cause < 0 || cause == i) continue;
            successors.get(cause).add(i);
            unmet[i]++;
        }
        List<Step> out = new ArrayList<>(n);
        boolean[] done = new boolean[n];
        java.util.PriorityQueue<Integer> ready = new java.util.PriorityQueue<>();   // index = clock order
        for (int i = 0; i < n; i++) if (unmet[i] == 0) ready.add(i);
        while (out.size() < n) {
            if (ready.isEmpty()) {
                for (int i = 0; i < n; i++) if (!done[i]) { ready.add(i); unmet[i] = 0; break; }   // cycle: clock wins
            }
            int i = ready.poll();
            if (done[i]) continue;
            done[i] = true;
            out.add(byClock.get(i));
            for (int j : successors.get(i)) if (--unmet[j] == 0 && !done[j]) ready.add(j);
        }
        return out;
    }

    /** The latest occurrence of {@code node} at or before step {@code i} in clock order, else the earliest after it. */
    private static int resolveCause(List<Step> byClock, int i, String node) {
        for (int k = i - 1; k >= 0; k--) if (node.equals(byClock.get(k).nodeId())) return k;
        for (int k = i + 1; k < byClock.size(); k++) if (node.equals(byClock.get(k).nodeId())) return k;
        return -1;
    }

    /** For every worker step, the worker steps that can reach it: what a causal hint may name. */
    static Map<String, Set<String>> predecessors(WorkflowDefinition def) {
        Map<String, Set<String>> out = new HashMap<>();
        for (Node from : def.nodes().values()) {
            if (!from.isWorkerDispatched()) continue;
            Set<String> seen = new HashSet<>();
            Deque<String> work = new ArrayDeque<>(successors(from));
            while (!work.isEmpty()) {
                String id = work.pop();
                if (id == null || !seen.add(id)) continue;
                Node to = def.nodes().get(id);
                if (to == null) continue;
                if (to.isWorkerDispatched()) out.computeIfAbsent(id, k -> new HashSet<>()).add(from.id());
                work.addAll(successors(to));
            }
        }
        return out;
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
                release(successorOf(node, s.predicateValue()));
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
            release(successorOf(node, s.predicateValue()));
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
