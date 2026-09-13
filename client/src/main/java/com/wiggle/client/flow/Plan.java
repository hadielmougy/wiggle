package com.wiggle.client.flow;

import com.wiggle.client.dsl.FlowSpec;
import com.wiggle.client.dsl.Branch;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.client.dsl.WorkflowBuilder;
import com.wiggle.core.RetryPolicy;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.UnaryOperator;

/**
 * What a flow definition records before it becomes a graph. Each {@code then*} call on a
 * {@link WiggleFlow} appends a {@link Step} to this tree instead of appending a node to the
 * builder, and {@link #compile} walks the tree once at the end.
 *
 * <p>The indirection exists for one reason: {@link Wiggle#allOf} takes handles that already exist,
 * so a handle must be continuable more than once -- the second continuation is what makes a fan-out.
 * A builder cannot express that, because by the time the second branch is written the first has
 * already been appended to the trunk. Recording first, compiling last, lets the fork be discovered
 * after both of its arms have been described.
 *
 * <p>A step with two unclaimed children is a fan-out that was never combined. The tree makes that
 * visible, and {@link #compile} rejects it by name -- the check the builder does at {@code build()}.
 */
final class Plan {

    private Plan() {}

    /** One recorded operation, and the steps recorded after it. */
    static class Step {

        final Step parent;
        /** How this step appends itself to a builder. Null on a root, which appends nothing. */
        private final UnaryOperator<WorkflowBuilder> op;
        /** The graph node this step adds, for arm naming and error messages. */
        final String label;
        final List<Step> children = new ArrayList<>();
        /** True once a fork has taken this step into one of its arms; it leaves the trunk. */
        boolean claimed;
        /** An explicit arm name from {@link WiggleFlow#named}, when this step ends an arm. */
        String armName;

        Step(Step parent, UnaryOperator<WorkflowBuilder> op, String label) {
            this.parent = parent;
            this.op = op;
            this.label = label;
            if (parent != null) parent.children.add(this);
        }

        WorkflowBuilder apply(WorkflowBuilder builder) {
            return op == null ? builder : op.apply(builder);
        }

        /** The name this step contributes when it ends a fork arm. */
        String armName() {
            if (armName != null) return armName;
            if (label != null) return label;
            throw new IllegalArgumentException(
                    "a fork arm needs a name: end it with .named(\"...\") so the combine handler's "
                    + "@Arm parameter has something to match");
        }
    }

    /** The step a {@link Wiggle#allOf} records: the arms it fans out into, and their merge. */
    static final class Fork extends Step {

        private final List<List<Step>> arms;
        final List<String> armNames;
        /** Set by the stage's combine(), before compilation reaches this step. */
        String combineName;

        Fork(Step parent, List<List<Step>> arms, List<String> armNames) {
            super(parent, null, null);
            this.arms = arms;
            this.armNames = armNames;
        }

        @Override
        WorkflowBuilder apply(WorkflowBuilder builder) {
            if (combineName == null) {
                throw new IllegalStateException("allOf(" + String.join(", ", armNames)
                        + ") has no merge: follow it with combine(...)");
            }
            Branch[] branches = new Branch[arms.size()];
            for (int i = 0; i < arms.size(); i++) {
                List<Step> arm = arms.get(i);
                String what = "arm '" + armNames.get(i) + "'";
                branches[i] = Branch.of(armNames.get(i), sub -> replay(arm, sub, what));
            }
            return builder.fork(branches).combine(combineName);
        }
    }

    // ------------------------------------------------------------------ recording

    static Step root() {
        return new Step(null, null, null);
    }

    /**
     * Records a fan-out over {@code leaves}: finds the step they all branched from, lifts the path to
     * each out of the trunk, and hangs a {@link Fork} where they diverged.
     */
    static Fork fork(List<Step> leaves) {
        if (leaves.size() < 2) throw new IllegalArgumentException("allOf needs at least two branches to fan out over");
        Step junction = junction(leaves);

        List<List<Step>> arms = new ArrayList<>(leaves.size());
        List<String> names = new ArrayList<>(leaves.size());
        for (Step leaf : leaves) {
            List<Step> path = pathFrom(junction, leaf);
            for (Step s : path) s.claimed = true;
            arms.add(path);
            names.add(leaf.armName());
        }
        if (Set.copyOf(names).size() != names.size()) {
            throw new IllegalArgumentException("allOf arms must have distinct names, got " + names
                    + " -- name them with .named(\"...\")");
        }
        return new Fork(junction, arms, names);
    }

    /** The nearest step every leaf descends from -- where the fork belongs. */
    private static Step junction(List<Step> leaves) {
        Set<Step> ancestors = new HashSet<>();
        for (Step s = leaves.get(0).parent; s != null; s = s.parent) ancestors.add(s);

        Step junction = null;
        for (int i = 1; i < leaves.size(); i++) {
            Step s = leaves.get(i).parent;
            while (s != null && !ancestors.contains(s)) s = s.parent;
            if (s == null) {
                throw new IllegalArgumentException(
                        "allOf was given handles from different definitions (or from inside a branch it does "
                        + "not enclose); every arm must fan out from one common point");
            }
            if (junction == null || isAncestor(junction, s)) junction = s;
        }
        Set<Step> onFirst = new HashSet<>();
        for (Step s = leaves.get(0); s != null; s = s.parent) onFirst.add(s);
        if (onFirst.contains(junction) && junction == leaves.get(0)) {
            throw new IllegalArgumentException(
                    "allOf was given a handle that is an ancestor of another; arms must be siblings");
        }
        return junction;
    }

    private static boolean isAncestor(Step maybeAncestor, Step step) {
        for (Step s = step; s != null; s = s.parent) if (s == maybeAncestor) return true;
        return false;
    }

    /** The steps from just below {@code junction} down to {@code leaf}, in order. */
    private static List<Step> pathFrom(Step junction, Step leaf) {
        List<Step> path = new ArrayList<>();
        for (Step s = leaf; s != junction; s = s.parent) {
            if (s == null) throw new IllegalStateException("leaf does not descend from the junction");
            if (s.claimed) {
                throw new IllegalArgumentException("the branch ending at '" + describe(s)
                        + "' is already an arm of another allOf");
            }
            path.add(s);
        }
        java.util.Collections.reverse(path);
        return path;
    }

    // ------------------------------------------------------------------ compilation

    /** Walks the recorded tree once, appending each step to a real builder. */
    static FlowSpec compile(String workflow, RetryPolicy defaultRetry, Step root) {
        WorkflowBuilder builder = defaultRetry == null
                ? Workflow.define(workflow) : Workflow.define(workflow, defaultRetry);
        return walk(root, builder, "workflow '" + workflow + "'").build();
    }

    /** Appends {@code from}'s descendants, following the single trunk at each step. */
    static WorkflowBuilder walk(Step from, WorkflowBuilder builder, String what) {
        Step step = from;
        while (true) {
            Step next = onlyChild(step, what);
            if (next == null) return builder;
            builder = next.apply(builder);
            step = next;
        }
    }

    /** Replays one arm's recorded path into a branch's own builder. */
    private static WorkflowBuilder replay(List<Step> arm, WorkflowBuilder builder, String what) {
        for (Step step : arm) {
            builder = step.apply(builder);
        }
        Step last = arm.get(arm.size() - 1);
        if (unclaimed(last).size() > 1) {
            throw new IllegalArgumentException(branchError(last, what));
        }
        // a single continuation after the arm's end is a step the caller added to the arm afterwards
        return walk(last, builder, what);
    }

    /** The one step to follow from here, or null at a tail; more than one is an uncombined fan-out. */
    private static Step onlyChild(Step step, String what) {
        List<Step> next = unclaimed(step);
        if (next.isEmpty()) return null;
        if (next.size() > 1) throw new IllegalArgumentException(branchError(step, what));
        return next.get(0);
    }

    private static List<Step> unclaimed(Step step) {
        List<Step> live = new ArrayList<>(step.children.size());
        for (Step child : step.children) if (!child.claimed) live.add(child);
        return live;
    }

    private static String branchError(Step step, String what) {
        List<String> heads = new ArrayList<>();
        for (Step child : unclaimed(step)) heads.add(describe(child));
        return "in " + what + ", the flow after " + describe(step) + " splits into " + heads
                + " but is never rejoined. Continuing one handle twice is a fan-out: pass both to "
                + "Wiggle.allOf(...) and combine them, or continue only one of them.";
    }

    private static String describe(Step step) {
        if (step.armName != null) return step.armName;
        if (step.label != null) return "'" + step.label + "'";
        return step.parent == null ? "the start of the flow" : "a fork";
    }
}
