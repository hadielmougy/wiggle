package com.wiggle.client.dsl;

import com.wiggle.core.*;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The accumulating build model behind the {@link WorkflowBuilder} DSL: it owns the graph's nodes and
 * edges, the queue set, and the workflow-level settings, and assembles them into an immutable
 * {@link Blueprint} on {@link #build()}. The blueprint is pure topology -- no step logic -- so this
 * only ever declares nodes (names, kinds, queues, retry); the implementations are bound separately
 * on a worker via {@link com.wiggle.client.worker.Handlers @Handlers} classes.
 */
final class Pipeline {

    private final String name;
    private final RetryPolicy defaultRetry;

    private final Map<String, Node> nodes = new LinkedHashMap<>();
    private final Set<String> queues = new LinkedHashSet<>();
    private final Set<String> stepNames = new LinkedHashSet<>();
    private final Set<String> checkpoints = new LinkedHashSet<>();

    private String startNode;
    private String defaultQueue;
    private ExecutionMode executionMode = ExecutionMode.DEFAULT;
    private int counter;

    Pipeline(String name, RetryPolicy defaultRetry) {
        if (name == null || name.isBlank()) throw new IllegalArgumentException("workflow name is required");
        this.name = name;
        this.defaultRetry = defaultRetry == null ? RetryPolicy.forever() : defaultRetry;
        this.defaultQueue = name;
    }

    void defaultQueue(String queue) { this.defaultQueue = Objects.requireNonNull(queue); }

    void executionMode(ExecutionMode mode) { this.executionMode = Objects.requireNonNull(mode, "mode"); }

    /** Records the graph's entry node. Called once, for the first node attached to the root stream. */
    void startAt(String id) { this.startNode = id; }

    /** Marks an already-added step as a checkpoint (see {@link WorkflowBuilder#checkpoint()}). */
    void markCheckpoint(String nodeId) { checkpoints.add(nodeId); }

    /** The step's own queue, or the workflow default when none is given. */
    private String queueOr(String queue) { return queue == null || queue.isBlank() ? defaultQueue : queue; }

    /** Reserves the step name and returns its activity id; the handler is bound on the worker by name. */
    private String reserveActivity(String name) {
        reserveName(name);
        return this.name + "#" + name;
    }

    /** A worker task node (step/effect); its handler is bound on the worker by name. */
    String addTask(String name, RetryPolicy retry, String queue) {
        String q = queueOr(queue);
        queues.add(q);
        return add(NodeDraft.task(name, reserveActivity(name), q, retryOr(retry)));
    }

    /** A worker predicate node (gate / choose guard / do-while condition); bound on the worker by name. */
    String addGuard(String name, RetryPolicy retry, String queue) {
        String q = queueOr(queue);
        queues.add(q);
        return add(NodeDraft.predicate(name, reserveActivity(name), q, retryOr(retry)));
    }

    /**
     * The mandatory merge node after a fork's join. It is a task node bound by name like any other,
     * but it carries the fork's arm names (a JSON array) on its {@code itemsKey} -- a field that
     * round-trips through every store -- so the engine can stage each isolated branch's result under
     * its name for the combine handler, and strip those scratch keys afterward.
     */
    String addCombine(String name, List<String> branchNames, RetryPolicy retry, String queue) {
        String id = addTask(name, retry, queue);
        nodes.put(id, nodes.get(id).withItemsKey(Json.write(List.copyOf(branchNames))));
        return id;
    }

    /** A server-side timer. Sleep names are not required to be unique (nothing addresses them). */
    String addSleep(String name, long millis) {
        return add(NodeDraft.sleep(name, millis));
    }

    /** A wait for the named signal ({@code deadlineMillis == 0} means no deadline). */
    String addSignal(String name, long deadlineMillis) {
        reserveName(name);
        return add(NodeDraft.signal(name, deadlineMillis));
    }

    /** A child-workflow node; its name is used for addressing and must be unique. */
    String addSubWorkflow(String name, String workflow) {
        reserveName(name);
        return add(NodeDraft.subWorkflow(name, workflow));
    }

    /** The runtime fan-out node; its name is used for addressing and must be unique. */
    String addDynFork(String name, String itemsKey, String itemKey) {
        reserveName(name);
        return add(NodeDraft.dynFork(name, itemsKey, itemKey));
    }

    String addFork() { return add(NodeDraft.fork("fork")); }

    /** A join barrier; {@code expected == 0} marks a dynamic join whose width travels in the group. */
    String addJoin(int expected) { return add(NodeDraft.join("join", expected)); }

    /** A successful terminal end. The DSL never builds a failed end; those arise at runtime. */
    String addEnd(String reason) { return add(NodeDraft.end(true, reason)); }

    /** Points {@code from}'s primary (true / next) edge at {@code target}. */
    void wireNext(String from, String target) { nodes.put(from, nodes.get(from).withNext(target)); }

    /** Points {@code from}'s alternate (false / escalation) edge at {@code target}. */
    void wireAlt(String from, String target) { nodes.put(from, nodes.get(from).withAltNext(target)); }

    /** Records a fork's (or dynamic fork's) branch start nodes. */
    void setBranches(String forkId, List<String> starts) {
        nodes.put(forkId, nodes.get(forkId).withBranches(starts));
    }

    /**
     * Assembles the accumulated nodes into a validated, content-addressed {@link Blueprint}. The
     * caller ({@link WorkflowBuilder#build()}) has already appended the terminal end node and wired
     * every open edge to it.
     */
    Blueprint build() {
        if (startNode == null) throw new IllegalStateException("workflow defines no steps");
        int version = WorkflowDefinition.contentVersion(name, startNode, nodes.values(), executionMode, checkpoints);
        WorkflowDefinition def = new WorkflowDefinition(
                name, version, startNode, Map.copyOf(nodes), Set.copyOf(queues), executionMode, copyOf(checkpoints));
        validate(def);
        return new Blueprint(def);
    }

    private Set<String> copyOf(Set<String> set) {
        return set == null ? Set.of() : Set.copyOf(set);
    }

    private RetryPolicy retryOr(RetryPolicy retry) {
        return retry != null ? retry : defaultRetry;
    }

    private String add(NodeDraft draft) {
        String id = nextId(prefix(draft.kind()));
        nodes.put(id, draft.toNode(id));
        return id;
    }

    private void reserveName(String name) {
        if (name == null || name.isBlank()) throw new IllegalArgumentException("step name is required");
        if (!stepNames.add(name)) {
            throw new IllegalArgumentException("duplicate step name '" + name + "' in workflow " + this.name);
        }
    }

    private String nextId(String prefix) {
        return prefix + (++counter);
    }

    private static String prefix(NodeKind kind) {
        return switch (kind) {
            case END      -> "end";
            case DYN_FORK -> "dynfork";
            case FORK     -> "fork";
            case JOIN     -> "join";
            default       -> "n";
        };
    }

    private static void validate(WorkflowDefinition def) {
        for (Node n : def.nodes().values()) {
            validateNode(def, n);
        }
    }

    private static void validateNode(WorkflowDefinition def, Node n) {
        requireKnownTarget(def, n, n.next());
        requireKnownTarget(def, n, n.altNext());
        switch (n.kind()) {
            case TASK, SLEEP, JOIN, SIGNAL, SUB_WORKFLOW -> requireSuccessor(n);
            case PREDICATE -> validatePredicate(n);
            case FORK -> validateFork(n);
            case DYN_FORK -> validateDynFork(n);
            case END -> { }
        }
    }

    private static void validateDynFork(Node n) {
        requireSuccessor(n);
        if (n.branches().size() != 1) {
            throw new IllegalStateException("dynamic fork " + n.id() + " needs exactly one branch template");
        }
        if (n.itemsKey() == null || n.itemKey() == null) {
            throw new IllegalStateException("dynamic fork " + n.id() + " is missing its items/item keys");
        }
    }

    private static void requireKnownTarget(WorkflowDefinition def, Node n, String target) {
        if (target != null && !def.nodes().containsKey(target)) {
            throw new IllegalStateException("node " + n.id() + " points at unknown node " + target);
        }
    }

    private static void requireSuccessor(Node n) {
        if (n.next() == null) throw new IllegalStateException("node " + n.id() + " has no successor");
    }

    private static void validatePredicate(Node n) {
        requireSuccessor(n);
        if (n.altNext() == null) {
            throw new IllegalStateException("predicate " + n.id() + " has no false branch");
        }
    }

    private static void validateFork(Node n) {
        if (n.branches().size() < 2) {
            throw new IllegalStateException("fork " + n.id() + " has fewer than two branches");
        }
    }
}
