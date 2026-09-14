package com.wiggle.client.flow;

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
        private final UnaryOperator<GraphBuilder> op;
        /** The graph node this step adds, for arm naming and error messages. */
        final String label;
        final List<Step> children = new ArrayList<>();
        /** True once a fork has taken this step into one of its arms; it leaves the trunk. */
        boolean claimed;

        Step(Step parent, UnaryOperator<GraphBuilder> op, String label) {
            this.parent = parent;
            this.op = op;
            this.label = label;
            if (parent != null) parent.children.add(this);
        }

        GraphBuilder apply(GraphBuilder builder) {
            return op == null ? builder : op.apply(builder);
        }
    }

    /** The step a {@link Wiggle#allOf} records: the arms it fans out into, and their merge. */
    static final class Fork extends Step {

        private final List<List<Step>> arms;
        final List<String> armNames;
        /** Set by the stage's combine(), before compilation reaches this step. */
        String combineName;

        /** A fan-out joins exactly once; a second combine on the same stage is a mistake, not an override. */
        void combine(String name) {
            if (combineName != null) {
                throw new IllegalStateException("allOf(" + String.join(", ", armNames)
                        + ") already has a merge ('" + combineName + "'); a fan-out joins once");
            }
            combineName = name;
        }

        Fork(Step parent, List<List<Step>> arms, List<String> armNames) {
            super(parent, null, null);
            this.arms = arms;
            this.armNames = armNames;
        }

        @Override
        GraphBuilder apply(GraphBuilder builder) {
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

    /**
     * The marker a {@link WiggleFlow#when} or {@link WiggleFlow#otherwise} records at the head of a
     * {@link Wiggle#oneOf} arm. It adds no node of its own -- a choose builds its guards itself, in
     * case order -- it only says which guard this arm is taken under.
     */
    static final class Guard extends Step {

        final String name;
        final RetryPolicy retry;
        final String queue;
        final boolean fallback;

        Guard(Step parent, String name, RetryPolicy retry, String queue, boolean fallback) {
            super(parent, null, null);
            this.name = name;
            this.retry = retry;
            this.queue = queue;
            this.fallback = fallback;
        }
    }

    /** The step a {@link Wiggle#oneOf} records: an exclusive choice over the arms it was given. */
    static final class Choice extends Step {

        private final List<Case> cases;

        Choice(Step parent, List<Case> cases) {
            super(parent, null, null);
            this.cases = cases;
        }

        @Override
        GraphBuilder apply(GraphBuilder builder) {
            return builder.choose(cases.toArray(new Case[0]));
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
            names.add(armName(path));
        }
        if (Set.copyOf(names).size() != names.size()) {
            throw new IllegalArgumentException("allOf arms must be distinguishable, but two of them end"
                    + " at steps with the same name " + names + ". Step names are unique, so this means"
                    + " an arm ends at a sleep (whose name need not be) -- give those sleeps different"
                    + " names with thenSleep(name, duration).");
        }
        return new Fork(junction, arms, names);
    }

    /**
     * Records an exclusive choice over {@code leaves} -- the same junction and claiming as
     * {@link #fork}, but each arm must open with a {@link WiggleFlow#when} or
     * {@link WiggleFlow#otherwise} marker, which says the guard it is taken under. The marker adds no
     * node; the choose builds its guards itself, in the order the arms were given, and the first to
     * hold wins.
     */
    static Choice choice(List<Step> leaves) {
        if (leaves.size() < 2) throw new IllegalArgumentException("oneOf needs at least two branches to choose between");
        Step junction = junction(leaves);

        List<Case> cases = new ArrayList<>(leaves.size());
        for (Step leaf : leaves) {
            List<Step> path = pathFrom(junction, leaf);
            for (Step s : path) s.claimed = true;

            if (!(path.get(0) instanceof Guard guard)) {
                throw new IllegalArgumentException(
                        "every arm of oneOf must open with when(...) or otherwise(): the arm ending at "
                        + describe(leaf) + " starts with " + describe(path.get(0))
                        + ". A choose picks one arm by evaluating each arm's guard in turn.");
            }
            List<Step> body = path.subList(1, path.size());
            if (body.isEmpty()) {
                throw new IllegalArgumentException("the " + (guard.fallback ? "otherwise" : "when('"
                        + guard.name + "')") + " arm of oneOf has no steps");
            }
            String what = guard.fallback ? "the otherwise arm" : "the '" + guard.name + "' arm";
            cases.add(guard.fallback
                    ? Case.otherwise("otherwise", sub -> replay(body, sub, what))
                    : Case.when(guard.name, guard.retry, guard.queue, sub -> replay(body, sub, what)));
        }
        return new Choice(junction, cases);
    }

    /**
     * What an arm is called: the name of the last step in it that has one, walking back past the
     * things that add no node of their own (a {@code withRetry}, a {@code compensate}). The engine
     * keys this arm's result by that name; a combine never names an arm, it takes them in order.
     *
     * <p>Deriving beats declaring here because step names are already unique within a workflow, so the
     * arm names are too, and there is nothing for the author to keep in sync.
     */
    private static String armName(List<Step> arm) {
        for (int i = arm.size() - 1; i >= 0; i--) {
            if (arm.get(i).label != null) return arm.get(i).label;
        }
        throw new IllegalArgumentException(
                "an allOf arm has no step to take its name from -- it must contain at least one step,"
                + " gate, combine or named sleep, since the engine keys the arm's result by that name");
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
        GraphBuilder builder = defaultRetry == null
                ? Workflow.define(workflow) : Workflow.define(workflow, defaultRetry);
        return walk(root, builder, "workflow '" + workflow + "'").build();
    }

    /** Appends {@code from}'s descendants, following the single trunk at each step. */
    static GraphBuilder walk(Step from, GraphBuilder builder, String what) {
        Step step = from;
        while (true) {
            Step next = onlyChild(step, what);
            if (next == null) return builder;
            builder = next.apply(builder);
            step = next;
        }
    }

    /** Replays one arm's recorded path into a branch's own builder. */
    private static GraphBuilder replay(List<Step> arm, GraphBuilder builder, String what) {
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
        if (step.label != null) return "'" + step.label + "'";
        return step.parent == null ? "the start of the flow" : "a fork";
    }
}
