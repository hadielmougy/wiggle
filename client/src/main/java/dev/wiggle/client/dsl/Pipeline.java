package dev.wiggle.client.dsl;

import dev.wiggle.core.*;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The accumulating build model behind the {@link WorkflowStream} DSL: it owns the graph's
 * nodes, their edges, the worker-side activity handlers, the queue set, and the workflow-level
 * settings, and it assembles them into an immutable {@link Blueprint} on {@link #build()}.
 *
 * <p>All state is private. Callers never see the collections; they add nodes through the
 * intention-revealing {@code addXxx} methods (each returns the new node's id), connect them
 * with {@link #wireNext}/{@link #wireAlt}, and read nothing back. A task/effect/guard is added
 * as one atomic operation that registers its handler, reserves its name, and records its queue
 * together, so the node and its handler can never drift apart.
 */
final class Pipeline<T> {

    private final String name;
    private final ContextCodec<T> codec;
    private final RetryPolicy defaultRetry;

    private final Map<String, Node> nodes = new LinkedHashMap<>();
    private final Map<String, ActivityHandler> handlers = new LinkedHashMap<>();
    private final Set<String> queues = new LinkedHashSet<>();
    private final Set<String> stepNames = new LinkedHashSet<>();
    private final Set<String> checkpoints = new LinkedHashSet<>();

    private String startNode;
    private String defaultQueue;
    private ExecutionMode executionMode = ExecutionMode.DEFAULT;
    private int counter;

    Pipeline(String name, ContextCodec<T> codec, RetryPolicy defaultRetry) {
        if (name == null || name.isBlank()) throw new IllegalArgumentException("workflow name is required");
        this.name = name;
        this.codec = codec;
        this.defaultRetry = defaultRetry == null ? RetryPolicy.forever() : defaultRetry;
        this.defaultQueue = name;
    }

    void defaultQueue(String queue) { this.defaultQueue = Objects.requireNonNull(queue); }

    void executionMode(ExecutionMode mode) { this.executionMode = Objects.requireNonNull(mode, "mode"); }

    /** Records the graph's entry node. Called once, for the first node attached to the root stream. */
    void startAt(String id) { this.startNode = id; }

    /** Marks an already-added step as a checkpoint (see {@link WorkflowStream#checkpoint()}). */
    void markCheckpoint(String nodeId) { checkpoints.add(nodeId); }

    /** A task node: {@code fn}'s result is diffed against the context and merged back. */
    String addStep(String name, Activity<T> fn, RetryPolicy retry) {
        return addWorkerTask(
                name,
                json -> Json.shallowDiff(json, codec.encode(fn.apply(codec.decode(json)))),
                retry);
    }

    /** A task node run for its side effect only; the context is left unchanged. */
    String addEffect(String name, SideEffect<T> fn, RetryPolicy retry) {
        return addWorkerTask(
                name,
                json -> { fn.accept(codec.decode(json)); return null; },
                retry);
    }

    /** A predicate node evaluated on a worker; its handler returns a {@link Boolean}. */
    String addGuard(String name, Predicate<T> test, RetryPolicy retry) {
        return addWorkerPredicate(name, json -> test.test(codec.decode(json)), retry);
    }

    private String addWorkerPredicate(String name, ActivityHandler handler, RetryPolicy retry) {
        queues.add(defaultQueue);
        NodeDraft draft = NodeDraft.predicate(name, registerActivity(name, handler), defaultQueue, retryOr(retry));
        return add(draft);
    }

    private String addWorkerTask(String name, ActivityHandler handler, RetryPolicy retry) {
        queues.add(defaultQueue);
        NodeDraft draft = NodeDraft.task(name, registerActivity(name, handler), defaultQueue, retryOr(retry));
        return add(draft);
    }

    /** Reserves the step name, registers its handler, and returns the derived activity id. */
    private String registerActivity(String name, ActivityHandler handler) {
        reserveName(name);
        String activity = this.name + "#" + name;
        handlers.put(activity, handler);
        return activity;
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

    /** Re-routes an already-added step to a dedicated queue (worker specialisation). */
    void setQueue(String nodeId, String queue) {
        nodes.put(nodeId, nodes.get(nodeId).withQueue(queue));
        queues.add(queue);
    }

    /**
     * Assembles the accumulated nodes into a validated, content-addressed {@link Blueprint}.
     * The caller ({@link WorkflowStream#build()}) has already appended the terminal end node and
     * wired every open edge to it.
     */
    Blueprint<T> build() {
        if (startNode == null) throw new IllegalStateException("workflow defines no steps");
        int version = WorkflowDefinition.contentVersion(name, startNode, nodes.values(), executionMode, checkpoints);
        WorkflowDefinition def = new WorkflowDefinition(
                name, version, startNode, Map.copyOf(nodes), Set.copyOf(queues), executionMode, copyOf(checkpoints));
        validate(def);
        return new Blueprint<>(def, handlers, codec);
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
