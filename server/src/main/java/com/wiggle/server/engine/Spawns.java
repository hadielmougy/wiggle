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
            String why = chainRefusal(b.steps(), limits);
            if (why != null) return "branch " + i + why;
        }
        if (keyed != 0 && keyed != branches.size()) {
            return keyed + " of " + branches.size() + " branches carry a key; either all do or none";
        }
        return null;
    }

    /** Why a chain of steps -- a branch, an arm, an escalation -- cannot run, or null. */
    private static String chainRefusal(List<BranchStep> steps, Limits limits) {
        if (steps.isEmpty()) return " has no steps";
        if (steps.size() > limits.maxSteps()) {
            return " has " + steps.size() + " steps, more than the " + limits.maxSteps()
                    + " allowed (WIGGLE_DYN_MAX_STEPS)";
        }
        for (int p = 0; p < steps.size(); p++) {
            String why = refusal(steps.get(p), limits);
            if (why != null) return ", step " + p + ": " + why;
        }
        return null;
    }

    private static String refusal(BranchStep step, Limits limits) {
        if (step.kind() == null) return "an unknown step kind";
        if (step.combine() != null && step.kind() != NodeKind.FORK
                && (step.kind() != NodeKind.TASK || step.combine().isBlank())) {
            return "only a task can create branches, and its combine needs a name";
        }
        return switch (step.kind()) {
            case PREDICATE -> step.isChoice() ? choiceRefusal(step, limits)
                    : step.isLoop() ? loopRefusal(step, limits)
                    : blank(step.name()) ? "a step needs a name" : null;
            case TASK -> blank(step.name()) ? "a step needs a name" : null;
            case SLEEP -> step.sleepMillis() < 0 ? "a negative sleep" : null;
            case SIGNAL -> signalRefusal(step, limits);
            case SUB_WORKFLOW -> blank(step.name()) || blank(step.workflow())
                    ? "a sub-flow needs a node name and a workflow" : null;
            case FORK -> forkRefusal(step, limits);
            default -> "a " + step.kind() + " step cannot run in a created branch";
        };
    }

    private static String signalRefusal(BranchStep step, Limits limits) {
        if (blank(step.name())) return "a wait needs the signal's name";
        if (step.sleepMillis() < 0) return "a negative timeout";
        if (step.escalation().isEmpty()) return null;
        if (step.sleepMillis() == 0) return "an escalation needs a timeout";
        String why = chainRefusal(step.escalation(), limits);
        return why == null ? null : "its escalation" + why;
    }

    private static String loopRefusal(BranchStep step, Limits limits) {
        if (blank(step.name())) return "a loop needs its condition's name";
        if (step.loopBudget() < 0) return "a loop's iteration budget cannot be negative";
        String why = chainRefusal(step.body(), limits);
        return why == null ? null : "its loop body" + why;
    }

    private static String choiceRefusal(BranchStep step, Limits limits) {
        List<BranchStep.Case> cases = step.cases();
        for (int c = 0; c < cases.size(); c++) {
            BranchStep.Case one = cases.get(c);
            if (one.guard() == null && c != cases.size() - 1) return "case " + c + ": the otherwise must come last";
            if (one.guard() != null && one.guard().isBlank()) return "case " + c + " needs its guard's name";
            if (one.steps().isEmpty()) continue;
            String why = chainRefusal(one.steps(), limits);
            if (why != null) return "case " + c + why;
        }
        return cases.getFirst().guard() == null ? "a choice needs a guarded case before its otherwise" : null;
    }

    /** A fork's arms are named by their last step, as in a definition, so each must end in a named step. */
    private static String forkRefusal(BranchStep step, Limits limits) {
        if (blank(step.combine())) return "a fork needs a combine";
        if (step.arms().size() < 2) return "a fork needs at least two arms";
        Set<String> names = new HashSet<>();
        for (int a = 0; a < step.arms().size(); a++) {
            List<BranchStep> arm = step.arms().get(a);
            String why = chainRefusal(arm, limits);
            if (why != null) return "arm " + a + why;
            BranchStep last = arm.getLast();
            if (last.kind() != NodeKind.TASK && last.kind() != NodeKind.PREDICATE) {
                return "arm " + a + " must end in a step: the arm is named after it";
            }
            if (!names.add(last.name())) return "two arms end in '" + last.name() + "'; an arm is named after its last step";
        }
        return null;
    }

    private static boolean blank(String s) {
        return s == null || s.isBlank();
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
            items.add(new FanOut.Item(compileChain(inst, creator, b.steps(), joinId, joinId, nodes), b.key(), b.input()));
        }
        tx.insertDynNodes(inst.id, nodes);
        return FanOut.items(tx, inst, fork, items, now);
    }

    /**
     * Compiles a chain into {@code nodes}, its last step going on to {@code next}, and answers its
     * entry. A false gate in it goes to {@code gateExit}, the join that closes the enclosing branch or
     * arm.
     */
    private static String compileChain(Instance inst, Node creator, List<BranchStep> steps, String next,
                                       String gateExit, List<Node> nodes) {
        for (int p = steps.size() - 1; p >= 0; p--) {   // last step first, so each knows where it goes
            next = compile(inst, creator, steps.get(p), next, gateExit, nodes);
        }
        return next;
    }

    /** How many nodes {@code branches} compile to: one per step, two more for each that creates its own. */
    static long nodeCount(List<CreatedBranch> branches) {
        long n = 0;
        for (CreatedBranch b : branches) n += chainNodeCount(b.steps());
        return n;
    }

    private static long chainNodeCount(List<BranchStep> steps) {
        long n = 0;
        for (BranchStep step : steps) {
            n += switch (step.kind()) {
                case TASK -> step.combine() == null ? 1 : 3;
                case PREDICATE -> step.isLoop() ? 1 + chainNodeCount(step.body())
                        : step.isChoice() ? step.cases().stream()
                                .mapToLong(c -> (c.guard() == null ? 0 : 1) + chainNodeCount(c.steps())).sum()
                        : 1;
                case SIGNAL -> 1 + chainNodeCount(step.escalation());
                case FORK -> 3 + step.arms().stream().mapToLong(Spawns::chainNodeCount).sum();
                default -> 1;
            };
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
     * Compiles one step into {@code nodes} and answers its entry. Each kind compiles as the same
     * construct does in a definition: a task that creates branches as a spawning step (the task, a
     * dynamic join, its combine), a fork as a fork with a static join and a combine over its arms.
     */
    private static String compile(Instance inst, Node creator, BranchStep step, String next, String gateExit,
                                  List<Node> nodes) {
        String id = newId();
        String queue = queue(creator, step);
        com.wiggle.core.RetryPolicy retry = retry(creator, step);
        switch (step.kind()) {
            case SLEEP -> nodes.add(Node.sleep(id, "sleep-" + step.sleepMillis() + "ms", step.sleepMillis()).withNext(next));
            case PREDICATE -> {
                if (step.isLoop()) return compileLoop(inst, creator, step, id, next, gateExit, nodes);
                if (step.isChoice()) return compileChoice(inst, creator, step, next, gateExit, nodes);
                nodes.add(worker(Node.predicate(id, step.name(), activity(inst, step.name()), queue, retry),
                        step).withNext(next).withAltNext(gateExit));
            }
            case SIGNAL -> {
                Node wait = Node.signal(id, step.name(), step.sleepMillis()).withNext(next);
                if (!step.escalation().isEmpty()) {
                    wait = wait.withAltNext(compileChain(inst, creator, step.escalation(), next, gateExit, nodes));
                }
                nodes.add(wait);
            }
            case SUB_WORKFLOW -> nodes.add(Node.subWorkflow(id, step.name(), step.workflow()).withNext(next));
            case FORK -> {
                Node combine = Node.task(newId(), step.combine(), activity(inst, step.combine()), queue, retry)
                        .withArmNames(step.arms().stream().map(arm -> arm.getLast().name()).toList())
                        .withNext(next);
                Node join = Node.join(newId(), "join", step.arms().size()).withNext(combine.id());
                List<String> starts = new ArrayList<>(step.arms().size());
                for (List<BranchStep> arm : step.arms()) {
                    starts.add(compileChain(inst, creator, arm, join.id(), join.id(), nodes));
                }
                nodes.add(Node.fork(id, "fork").withBranches(starts));
                nodes.add(join);
                nodes.add(combine);
            }
            default -> {
                Node task = worker(Node.task(id, step.name(), activity(inst, step.name()), queue, retry), step);
                if (step.combine() == null) {
                    nodes.add(task.withNext(next));
                } else {
                    Node combine = Node.task(newId(), step.combine(), activity(inst, step.combine()), queue, retry)
                            .withCollectKey(ScratchKeys.spawn(step.name()))
                            .withNext(next);
                    Node join = Node.join(newId(), "join", 0).withNext(combine.id());
                    nodes.add(task.withNext(join.id()));
                    nodes.add(join);
                    nodes.add(combine);
                }
            }
        }
        return id;
    }

    /** A do-while, as in a definition: the body first, then the condition, true back to the body. */
    private static String compileLoop(Instance inst, Node creator, BranchStep step, String id, String next,
                                      String gateExit, List<Node> nodes) {
        String bodyEntry = compileChain(inst, creator, step.body(), id, gateExit, nodes);
        nodes.add(Node.predicate(id, step.name(), activity(inst, step.name()), queue(creator, step), retry(creator, step))
                .withLoopBudget(step.loopBudget() == 0 ? -1 : step.loopBudget())
                .withNext(bodyEntry)
                .withAltNext(next));
        return bodyEntry;
    }

    /**
     * A choice, as in a definition: a chain of guards, each true into its case and false into the
     * next guard; the last falls through to the otherwise, or past the choice.
     */
    private static String compileChoice(Instance inst, Node creator, BranchStep step, String next, String gateExit,
                                        List<Node> nodes) {
        List<BranchStep.Case> cases = step.cases();
        String fallthrough = next;
        int guarded = cases.size();
        if (cases.getLast().guard() == null) {
            fallthrough = caseEntry(inst, creator, cases.getLast(), next, gateExit, nodes);
            guarded--;
        }
        for (int c = guarded - 1; c >= 0; c--) {
            BranchStep.Case one = cases.get(c);
            String guardId = newId();
            nodes.add(Node.predicate(guardId, one.guard(), activity(inst, one.guard()), queue(creator, step),
                            retry(creator, step))
                    .withNext(caseEntry(inst, creator, one, next, gateExit, nodes))
                    .withAltNext(fallthrough));
            fallthrough = guardId;
        }
        return fallthrough;
    }

    private static String caseEntry(Instance inst, Node creator, BranchStep.Case one, String next, String gateExit,
                                    List<Node> nodes) {
        return one.steps().isEmpty() ? next : compileChain(inst, creator, one.steps(), next, gateExit, nodes);
    }

    private static String newId() {
        return CreatedBranch.NODE_PREFIX + Ids.token();
    }

    private static String activity(Instance inst, String step) {
        return inst.workflow + "#" + step;
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
