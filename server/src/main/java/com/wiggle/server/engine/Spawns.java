package com.wiggle.server.engine;

import com.wiggle.core.CreatedBranch;
import com.wiggle.core.CreatedBranch.BranchStep;
import com.wiggle.core.Doc;
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
    record Limits(long maxBranches, long maxSteps, long maxRounds) {

        static Limits fromEnv() {
            return new Limits(
                    ServerEnv.envLong("wiggle.dyn.maxBranches", "WIGGLE_DYN_MAX_BRANCHES", 10_000),
                    ServerEnv.envLong("wiggle.dyn.maxSteps", "WIGGLE_DYN_MAX_STEPS", 100),
                    ServerEnv.envLong("wiggle.dyn.maxRounds", "WIGGLE_DYN_MAX_ROUNDS", 100));
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
        List<String> starts = new ArrayList<>(branches.size());
        for (CreatedBranch b : branches) {
            List<String> ids = new ArrayList<>(b.steps().size());
            for (int p = 0; p < b.steps().size(); p++) ids.add(CreatedBranch.NODE_PREFIX + Ids.token());
            for (int p = 0; p < b.steps().size(); p++) {
                String next = p + 1 < ids.size() ? ids.get(p + 1) : joinId;
                nodes.add(compile(inst, creator, b.steps().get(p), ids.get(p), next, joinId));
            }
            starts.add(ids.getFirst());
        }
        tx.insertDynNodes(inst.id, nodes);
        String group = fork.id + "#" + branches.size();
        String childStack = fork.pushJoinStack(group);
        List<Token> children = new ArrayList<>(branches.size());
        for (int i = 0; i < branches.size(); i++) {
            CreatedBranch b = branches.get(i);
            Token child = Tokens.create(inst, starts.get(i), childStack,
                    fork.payload.push(TokenPayload.FrameKind.ITEM, i, b.key(), Doc.of(b.input())), now);
            tx.insertToken(child);
            children.add(child);
        }
        return children;
    }

    /** The join a later round's branches meet at, leading back to {@code combine}. */
    static Node roundJoin(Node combine) {
        return Node.join(CreatedBranch.NODE_PREFIX + Ids.token(), "join", 0).withNext(combine.id());
    }

    /** What a combine receives when its step created nothing: an empty collection. */
    static TokenPayload emptyRound(Token fork, Node combine) {
        return fork.payload.withStaged(Map.of(combine.collectKey(), List.of()));
    }

    private static Node compile(Instance inst, Node creator, BranchStep step, String id, String next,
                                String joinId) {
        return switch (step.kind()) {
            case SLEEP -> Node.sleep(id, "sleep-" + step.sleepMillis() + "ms", step.sleepMillis()).withNext(next);
            case PREDICATE -> worker(Node.predicate(id, step.name(), inst.workflow + "#" + step.name(),
                    queue(creator, step), retry(creator, step)), step).withNext(next).withAltNext(joinId);
            default -> worker(Node.task(id, step.name(), inst.workflow + "#" + step.name(),
                    queue(creator, step), retry(creator, step)), step).withNext(next);
        };
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
