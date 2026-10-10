package com.wiggle.server.engine;

import com.wiggle.core.CreatedBranch;
import com.wiggle.core.CreatedBranch.BranchStep;
import com.wiggle.core.GraphTraversal;
import com.wiggle.core.Ids;
import com.wiggle.core.Node;
import com.wiggle.core.NodeKind;
import com.wiggle.core.ScratchKeys;
import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.Rows.Token;
import com.wiggle.server.store.TokenPayload;
import com.wiggle.server.store.Tx;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Branches a step creates at run time ({@code Step.create}). A spawning step is a task followed
 * directly by a dynamic join and the combine that collects under {@link ScratchKeys#spawn}; its
 * report's branches compile to nodes owned by the instance, whose tokens fan out as a forEach's do
 * and meet at that join. A combine of a spawning step that creates branches starts another round,
 * which meets at a join created for it and runs the same combine again.
 */
final class Spawns {

    /** The bounds on what one instance may create. */
    record Limits(long maxBranches, long maxSteps, long maxRounds, long maxNodes, long maxDepth) {

        static Limits fromEnv() {
            return new Limits(
                    ServerEnv.envLong("wiggle.dyn.maxBranches", "WIGGLE_DYN_MAX_BRANCHES", 10_000),
                    ServerEnv.envLong("wiggle.dyn.maxSteps", "WIGGLE_DYN_MAX_STEPS", 100),
                    ServerEnv.envLong("wiggle.dyn.maxRounds", "WIGGLE_DYN_MAX_ROUNDS", 100),
                    ServerEnv.envLong("wiggle.dyn.maxNodes", "WIGGLE_DYN_MAX_NODES", 100_000),
                    ServerEnv.envLong("wiggle.dyn.maxDepth", "WIGGLE_DYN_MAX_DEPTH", 16));
        }
    }

    private Spawns() {}

    static boolean isCreated(String nodeId) {
        return CreatedBranch.isCreatedNode(nodeId);
    }

    /** The combine a spawning step feeds, or empty when {@code node} is not a spawning step. */
    static Optional<Node> combineOf(LazyGraph def, Node node) {
        return GraphTraversal.spawnCombine(node, def::find);
    }

    /** Whether {@code node} is the combine of a spawning step, which may start another round. */
    static boolean isRoundCombine(Node node) {
        return node.isCombine() && ScratchKeys.isSpawn(node.collectKey());
    }

    /** Why {@code branches} cannot be accepted, or null when they can. */
    static String refusal(List<CreatedBranch> branches, Limits limits) {
        if (branches.size() > limits.maxBranches()) {
            return "created " + branches.size() + " branches, more than the " + limits.maxBranches()
                    + " allowed (WIGGLE_DYN_MAX_BRANCHES)";
        }
        int keyed = 0;
        Set<String> keys = new HashSet<>();
        for (int i = 0; i < branches.size(); i++) {
            CreatedBranch b = branches.get(i);
            if (b.key() != null) {
                keyed++;
                if (!keys.add(b.key())) return "branch " + i + " repeats the key '" + b.key() + "'";
            }
            if (b.steps().isEmpty()) return "branch " + i + " has no steps";
            if (b.steps().size() > limits.maxSteps()) {
                return "branch " + i + " has " + b.steps().size() + " steps, more than the "
                        + limits.maxSteps() + " allowed (WIGGLE_DYN_MAX_STEPS)";
            }
            for (int p = 0; p < b.steps().size(); p++) {
                String why = refusal(b.steps().get(p));
                if (why != null) return "branch " + i + ", step " + p + ": " + why;
            }
        }
        if (keyed != 0 && keyed != branches.size()) {
            return keyed + " of " + branches.size() + " branches carry a key; either all do or none";
        }
        return null;
    }

    private static String refusal(BranchStep step) {
        if (step.kind() == null) return "an unknown step kind";
        if (step.combine() != null && (step.kind() != NodeKind.TASK || step.combine().isBlank())) {
            return "only a task can create branches, and its combine needs a name";
        }
        return switch (step.kind()) {
            case TASK, PREDICATE -> step.name() == null || step.name().isBlank() ? "a step needs a name" : null;
            case SLEEP -> step.sleepMillis() < 0 ? "a negative sleep" : null;
            default -> "a " + step.kind() + " step cannot run in a created branch";
        };
    }

    /**
     * Compiles {@code branches} into nodes owned by {@code inst}, each branch ending at
     * {@code joinId}, and mints one token per branch from {@code fork}, whose payload is the base the
     * join restores. Steps without their own queue or retry policy take {@code creator}'s.
     */
    static List<Token> fanOut(Tx tx, Instance inst, Token fork, Node creator, String joinId,
                              List<CreatedBranch> branches, List<Node> extraNodes, long now) {
        List<Node> nodes = new ArrayList<>(extraNodes);
        List<FanOut.Item> items = new ArrayList<>(branches.size());
        for (CreatedBranch b : branches) {
            String next = joinId;   // compiled last step first, so each knows where it goes
            for (int p = b.steps().size() - 1; p >= 0; p--) {
                List<Node> step = compile(inst, creator, b.steps().get(p), next, joinId);
                nodes.addAll(step);
                next = step.getFirst().id();
            }
            items.add(new FanOut.Item(next, b.key(), b.input()));
        }
        tx.insertDynNodes(inst.id, nodes);
        return FanOut.items(tx, inst, fork, items, now);
    }

    /** How many nodes {@code branches} compile to: one per step, two more for each that creates its own. */
    static long nodeCount(List<CreatedBranch> branches) {
        long n = 0;
        for (CreatedBranch b : branches) {
            for (BranchStep step : b.steps()) n += step.combine() == null ? 1 : 3;
        }
        return n;
    }

    /** The join a later round's branches meet at, leading back to {@code combine}. */
    static Node roundJoin(Node combine) {
        return Node.join(CreatedBranch.NODE_PREFIX + Ids.token(), "join", 0).withNext(combine.id());
    }

    /** What a combine receives when its step created nothing: an empty collection. */
    static TokenPayload emptyRound(Token fork, Node combine) {
        return fork.payload.withStaged(Map.of(combine.collectKey(), List.of()));
    }

    /**
     * The nodes one step compiles to, its entry first. A task that creates branches of its own
     * compiles as a spawning step does in a definition: the task, a dynamic join, and its combine,
     * which goes on to {@code next}.
     */
    private static List<Node> compile(Instance inst, Node creator, BranchStep step, String next, String joinId) {
        String id = CreatedBranch.NODE_PREFIX + Ids.token();
        if (step.kind() == NodeKind.SLEEP) {
            return List.of(Node.sleep(id, "sleep-" + step.sleepMillis() + "ms", step.sleepMillis()).withNext(next));
        }
        String queue = queue(creator, step);
        com.wiggle.core.RetryPolicy retry = retry(creator, step);
        if (step.kind() == NodeKind.PREDICATE) {
            return List.of(worker(Node.predicate(id, step.name(), inst.workflow + "#" + step.name(), queue, retry),
                    step).withNext(next).withAltNext(joinId));
        }
        Node task = worker(Node.task(id, step.name(), inst.workflow + "#" + step.name(), queue, retry), step);
        if (step.combine() == null) return List.of(task.withNext(next));
        Node combine = Node.task(CreatedBranch.NODE_PREFIX + Ids.token(), step.combine(),
                        inst.workflow + "#" + step.combine(), queue, retry)
                .withCollectKey(ScratchKeys.spawn(step.name()))
                .withNext(next);
        Node join = Node.join(CreatedBranch.NODE_PREFIX + Ids.token(), "join", 0).withNext(combine.id());
        return List.of(task.withNext(join.id()), join, combine);
    }

    private static Node worker(Node node, BranchStep step) {
        return step.compensable() ? node.withCompensable() : node;
    }

    private static String queue(Node creator, BranchStep step) {
        return step.queue() != null ? step.queue() : creator.queue();
    }

    private static com.wiggle.core.RetryPolicy retry(Node creator, BranchStep step) {
        return step.retry() != null ? step.retry() : creator.retry();
    }
}
