package dev.wiggle.client.dsl;

import dev.wiggle.core.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

/**
 * A lazily-built workflow pipeline with a Stream-shaped API: intermediate operations
 * append nodes and return a stream, and the terminal {@link #build()} produces the
 * artifact. Nothing executes at definition time.
 *
 * One deliberate departure from {@code java.util.stream}: the context type does not
 * change from step to step. A workflow context is a durable document that survives
 * process restarts and is merged across parallel branches, so a single type for the
 * whole pipeline is what actually models the storage. {@code map} is therefore closer
 * to {@code UnaryOperator<T>} than to {@code Function<T,R>}.
 */
public final class WorkflowStream<T> {

    private static final int NEXT = 0;
    private static final int ALT_NEXT = 1;

    private final Pipeline<T> pipeline;
    private final Consumer<String> startSink;
    /** Non-null when this stream is a fork branch: where a short-circuited branch must land. */
    private final String enclosingJoinId;
    private List<int[]> openSlots = new ArrayList<>();   // parallel arrays with openNodes
    private List<String> openNodes = new ArrayList<>();
    private String lastStepId;
    private boolean consumed;

    private WorkflowStream(Pipeline<T> pipeline, Consumer<String> startSink, String enclosingJoinId) {
        this.pipeline = pipeline;
        this.startSink = startSink;
        this.enclosingJoinId = enclosingJoinId;
    }

    static <T> WorkflowStream<T> root(Pipeline<T> pipeline) {
        return new WorkflowStream<>(pipeline, id -> pipeline.startNode = id, null);
    }

    /** A unit of work run on a worker: {@code fn}'s result becomes the new context. */
    public WorkflowStream<T> step(String name, Activity<T> fn) {
        return step(name, fn, null);
    }

    /**
     * A unit of work run on a worker, with an explicit retry policy. Pass {@code retry} to
     * govern how a thrown step is retried (exponential backoff by default); a {@code null}
     * policy falls back to the workflow default.
     */
    public WorkflowStream<T> step(String name, Activity<T> fn, RetryPolicy retry) {
        Objects.requireNonNull(fn, "activity");
        String activity = pipeline.activityFor(name);
        ContextCodec<T> codec = pipeline.codec;
        pipeline.handlers.put(activity,
                json -> dev.wiggle.core.Json.shallowDiff(json, codec.encode(fn.apply(codec.decode(json)))));
        String id = pipeline.nextId("n");
        pipeline.put(Node.task(id, name, activity, pipeline.defaultQueue, retryOr(retry)));
        pipeline.queues.add(pipeline.defaultQueue);
        attach(id);
        lastStepId = id;
        return this;
    }

    /** Alias for {@link #step} that reads well when sequencing ("do this, then that"). */
    public WorkflowStream<T> then(String name, Activity<T> fn) {
        return step(name, fn, null);
    }

    /** Alias for {@link #step(String, Activity, RetryPolicy)}. */
    public WorkflowStream<T> then(String name, Activity<T> fn, RetryPolicy retry) {
        return step(name, fn, retry);
    }

    /** Runs {@code fn} on a worker for its side effect only; the context is left unchanged. */
    public WorkflowStream<T> effect(String name, SideEffect<T> fn) {
        return effect(name, fn, null);
    }

    /** {@link #effect} with an explicit retry policy; a {@code null} policy uses the workflow default. */
    public WorkflowStream<T> effect(String name, SideEffect<T> fn, RetryPolicy retry) {
        Objects.requireNonNull(fn, "side effect");
        String activity = pipeline.activityFor(name);
        ContextCodec<T> codec = pipeline.codec;
        pipeline.handlers.put(activity, json -> {
            fn.accept(codec.decode(json));
            return null;
        });
        String id = pipeline.nextId("n");
        pipeline.put(Node.task(id, name, activity, pipeline.defaultQueue, retryOr(retry)));
        pipeline.queues.add(pipeline.defaultQueue);
        attach(id);
        lastStepId = id;
        return this;
    }

    /** {@link #gate} whose guard uses the workflow's default retry policy. */
    public WorkflowStream<T> gate(String name, Predicate<T> test) {
        return gate(name, test, null);
    }

    /**
     * A guard evaluated on a worker: the flow continues only while {@code test} holds. A false
     * result ends the instance successfully with the termination reason {@code "gated:<name>"}
     * -- the workflow equivalent of an empty stream, not an error.
     *
     * <p>Inside a fork branch the false path short-circuits to the enclosing join instead, so
     * the branch still arrives at the barrier and its siblings are not stranded. A {@code null}
     * retry policy uses the workflow default.
     */
    public WorkflowStream<T> gate(String name, Predicate<T> test, RetryPolicy retry) {
        Objects.requireNonNull(test, "predicate");
        String activity = pipeline.activityFor(name);
        ContextCodec<T> codec = pipeline.codec;
        pipeline.handlers.put(activity, json -> test.test(codec.decode(json)));
        String id = pipeline.nextId("n");
        pipeline.put(Node.predicate(id, name, activity, pipeline.defaultQueue, retryOr(retry)));
        pipeline.queues.add(pipeline.defaultQueue);
        attach(id);

        if (enclosingJoinId != null) {
            pipeline.wire(id, ALT_NEXT, enclosingJoinId);
        } else {
            String stop = pipeline.nextId("n");
            pipeline.put(Node.end(stop, true, "gated:" + name));
            pipeline.wire(id, ALT_NEXT, stop);
        }

        openNodes = new ArrayList<>(List.of(id));
        openSlots = new ArrayList<>(List.of(new int[]{NEXT}));
        lastStepId = id;
        return this;
    }

    private RetryPolicy retryOr(RetryPolicy retry) {
        return retry != null ? retry : pipeline.defaultRetry;
    }

    /** Server-side timer. No worker is occupied while the instance waits. */
    public WorkflowStream<T> sleep(Duration duration) {
        return sleep("sleep-" + duration.toMillis() + "ms", duration);
    }

    public WorkflowStream<T> sleep(String stepName, Duration duration) {
        if (duration.isNegative()) throw new IllegalArgumentException("sleep duration must not be negative");
        pipeline.stepNames.add(stepName);
        String id = pipeline.nextId("n");
        pipeline.put(Node.sleep(id, stepName, duration.toMillis()));
        attach(id);
        lastStepId = null;
        return this;
    }

    /**
     * Waits for an external actor (a human, or another system) to complete the task out of
     * band -- the flow then continues down the following step. No worker is held while it
     * waits. Complete it via the control API / dashboard ({@code POST /api/tasks/{id}/complete});
     * the submitted result merges into the context like a {@link #step}.
     */
    public WorkflowStream<T> userTask(String name) {
        return userTask(name, null, null);
    }

    /**
     * A user task with a deadline. If nobody completes it within {@code timeout}, the instance
     * fails with a timeout error. Use {@link #userTask(String, Duration, java.util.function.UnaryOperator)}
     * to escalate to a branch instead.
     */
    public WorkflowStream<T> userTask(String name, Duration timeout) {
        return userTask(name, timeout, null);
    }

    /**
     * A user task with a deadline and an escalation branch: if it is not completed within
     * {@code timeout}, the {@code escalation} branch runs instead, then rejoins the flow after
     * the task (exactly one of completion / escalation happens).
     */
    public WorkflowStream<T> userTask(String name, Duration timeout,
                                      java.util.function.UnaryOperator<WorkflowStream<T>> escalation) {
        if (timeout != null && timeout.isNegative()) throw new IllegalArgumentException("timeout must not be negative");
        if (timeout == null && escalation != null) throw new IllegalArgumentException("escalation needs a timeout");
        if (!pipeline.stepNames.add(name)) {
            throw new IllegalArgumentException("duplicate step name '" + name + "' in workflow " + pipeline.name);
        }
        String id = pipeline.nextId("n");
        pipeline.put(Node.userTask(id, name, timeout == null ? 0 : timeout.toMillis()));
        attach(id);
        // The completion path (slot NEXT) is the open end; the escalation branch hangs off ALT_NEXT.
        openNodes = new ArrayList<>(List.of(id));
        openSlots = new ArrayList<>(List.of(new int[]{NEXT}));
        lastStepId = null;

        if (escalation != null) {
            String[] start = new String[1];
            WorkflowStream<T> sub = new WorkflowStream<>(pipeline, s -> start[0] = s, enclosingJoinId);
            WorkflowStream<T> tail = escalation.apply(sub);
            if (start[0] == null) throw new IllegalArgumentException("escalation branch of '" + name + "' defines no steps");
            pipeline.wire(id, ALT_NEXT, start[0]);
            openNodes.addAll(tail.openNodes);
            openSlots.addAll(tail.openSlots);
        }
        return this;
    }

    /**
     * Fans out into parallel branches and waits for all of them. Branches execute
     * independently and may be picked up by different workers. Each step writes back
     * only the context fields it actually changed, so branches touching different
     * fields merge cleanly; if two branches write the same field, the later write wins.
     */
    @SafeVarargs
    public final WorkflowStream<T> fork(Branch<T>... branches) {
        if (branches.length < 2) throw new IllegalArgumentException("fork needs at least two branches");
        String forkId = pipeline.nextId("fork");
        pipeline.put(Node.fork(forkId, "fork"));
        attach(forkId);

        String joinId = pipeline.nextId("join");
        pipeline.put(Node.join(joinId, "join", branches.length));

        List<String> starts = new ArrayList<>(branches.length);
        for (Branch<T> branch : branches) {
            starts.add(buildBranch(branch, joinId));
        }
        pipeline.put(pipeline.get(forkId).withBranches(starts));

        openNodes = new ArrayList<>(List.of(joinId));
        openSlots = new ArrayList<>(List.of(new int[]{NEXT}));
        lastStepId = null;
        return this;
    }

    /**
     * Runtime fan-out: when the instance reaches this node, the engine reads the list stored in
     * the context under {@code itemsKey} and spawns one parallel branch per element, each running
     * {@code body} with its element injected into the context under {@code itemKey} (and its
     * position under {@code itemKey + "Index"}) -- visible only within that branch. All branches
     * join before the flow continues; an empty or missing list skips straight through.
     *
     * <p>Branch writes merge into the shared context like a static {@link #fork}: last write to
     * the same key wins, so per-element results belong under per-element keys (use the index).
     */
    public WorkflowStream<T> forkEach(String name, String itemsKey, String itemKey,
                                      UnaryOperator<WorkflowStream<T>> body) {
        Objects.requireNonNull(itemsKey, "itemsKey");
        Objects.requireNonNull(itemKey, "itemKey");
        if (!pipeline.stepNames.add(name)) {
            throw new IllegalArgumentException("duplicate step name '" + name + "' in workflow " + pipeline.name);
        }
        String forkId = pipeline.nextId("dynfork");
        pipeline.put(Node.dynFork(forkId, name, itemsKey, itemKey));
        attach(forkId);
        String joinId = pipeline.nextId("join");
        pipeline.put(Node.join(joinId, "join", 0));   // 0 = dynamic width, carried in the join group
        String templateStart = buildBranch(Branch.of(name, body), joinId);
        pipeline.put(pipeline.get(forkId).withBranches(List.of(templateStart)).withNext(joinId));
        openNodes = new ArrayList<>(List.of(joinId));
        openSlots = new ArrayList<>(List.of(new int[]{NEXT}));
        lastStepId = null;
        return this;
    }

    /**
     * A do-while loop: runs {@code body} once, then evaluates {@code condition} on a worker;
     * while it holds, the body runs again. Compiles to a plain cycle in the graph -- the
     * condition is an ordinary predicate whose true edge points back at the body -- so it works
     * identically under every execution mode (a local chain simply keeps iterating in-worker).
     */
    public WorkflowStream<T> doWhile(String conditionName, Predicate<T> condition,
                                     UnaryOperator<WorkflowStream<T>> body) {
        Objects.requireNonNull(condition, "condition");
        String[] start = new String[1];
        WorkflowStream<T> sub = new WorkflowStream<>(pipeline, id -> start[0] = id, enclosingJoinId);
        WorkflowStream<T> tail = body.apply(sub);
        if (start[0] == null) throw new IllegalArgumentException("doWhile body defines no steps");

        ContextCodec<T> codec = pipeline.codec;
        String activity = pipeline.activityFor(conditionName);
        pipeline.handlers.put(activity, json -> condition.test(codec.decode(json)));
        String condId = pipeline.nextId("n");
        pipeline.put(Node.predicate(condId, conditionName, activity, pipeline.defaultQueue, pipeline.defaultRetry));
        pipeline.queues.add(pipeline.defaultQueue);

        // Enter at the body; body tail feeds the condition; true loops, false continues onward.
        if (openNodes.isEmpty()) startSink.accept(start[0]); else wireOpenEndsTo(start[0]);
        tail.wireOpenEndsTo(condId);
        pipeline.wire(condId, NEXT, start[0]);
        openNodes = new ArrayList<>(List.of(condId));
        openSlots = new ArrayList<>(List.of(new int[]{ALT_NEXT}));
        lastStepId = null;
        return this;
    }

    /** Builds one fork branch as a sub-stream wired to the join; returns its start node id. */
    private String buildBranch(Branch<T> branch, String joinId) {
        String[] start = new String[1];
        WorkflowStream<T> sub = new WorkflowStream<>(pipeline, id -> start[0] = id, joinId);
        WorkflowStream<T> tail = branch.body().apply(sub);
        if (start[0] == null) {
            throw new IllegalArgumentException("branch '" + branch.name() + "' defines no steps");
        }
        tail.wireOpenEndsTo(joinId);
        return start[0];
    }

    /**
     * Exclusive choice -- a switch/case over the context. Each case's guard is evaluated in
     * order and the first one to hold runs its branch; the rest are skipped. If no guard
     * matches, control passes to an {@link Case#otherwise} branch when one is given, otherwise
     * straight to the step after {@code choose}. Exactly one branch ever runs.
     *
     * <p>Unlike {@link #fork}, nothing runs in parallel and there is no join: it is a cascade
     * of predicates, so a five-way choose costs at most five guard evaluations, short-circuiting
     * at the first match.
     */
    @SafeVarargs
    public final WorkflowStream<T> choose(Case<T>... cases) {
        List<Case<T>> all = new ArrayList<>(cases.length);
        for (Case<T> c : cases) all.add(c);
        boolean hasDefault = validateChoose(all);
        int guards = hasDefault ? all.size() - 1 : all.size();

        // Lay down the guard predicates first, so each false path can point at the next guard.
        String[] guardIds = new String[guards];
        for (int i = 0; i < guards; i++) {
            guardIds[i] = addGuardNode(all.get(i));
        }

        // Enter at the first guard, then take over the open-end bookkeeping ourselves.
        attach(guardIds[0]);
        openNodes = new ArrayList<>();
        openSlots = new ArrayList<>();

        // Chain each guard's false path to the next guard.
        for (int i = 0; i < guards - 1; i++) {
            pipeline.wire(guardIds[i], ALT_NEXT, guardIds[i + 1]);
        }

        // Each guard's true path runs its branch; the branch's open ends become the choose's.
        for (int i = 0; i < guards; i++) {
            collectCaseBranch(all.get(i), guardIds[i], NEXT);
        }

        wireChooseFallthrough(all, guardIds, hasDefault);
        lastStepId = null;
        return this;
    }

    private boolean validateChoose(List<Case<T>> cases) {
        if (cases.isEmpty()) throw new IllegalArgumentException("choose needs at least one case");
        for (int i = 0; i < cases.size() - 1; i++) {
            if (cases.get(i).guard() == null) throw new IllegalArgumentException("otherwise() must be the last case");
        }
        boolean hasDefault = cases.get(cases.size() - 1).guard() == null;
        if (hasDefault && cases.size() == 1) {
            throw new IllegalArgumentException("choose needs at least one guarded case");
        }
        return hasDefault;
    }

    /** Adds one guard as a predicate node and registers its handler; returns the node id. */
    private String addGuardNode(Case<T> c) {
        Predicate<T> guard = c.guard();
        ContextCodec<T> codec = pipeline.codec;
        String activity = pipeline.activityFor(c.name());
        pipeline.handlers.put(activity, json -> guard.test(codec.decode(json)));
        String id = pipeline.nextId("n");
        pipeline.put(Node.predicate(id, c.name(), activity, pipeline.defaultQueue, pipeline.defaultRetry));
        pipeline.queues.add(pipeline.defaultQueue);
        return id;
    }

    /** The last false path: a default branch, or an open end that skips the choose entirely. */
    private void wireChooseFallthrough(List<Case<T>> cases, String[] guardIds, boolean hasDefault) {
        String lastGuard = guardIds[guardIds.length - 1];
        if (hasDefault) {
            collectCaseBranch(cases.get(cases.size() - 1), lastGuard, ALT_NEXT);
            return;
        }
        openNodes.add(lastGuard);
        openSlots.add(new int[]{ALT_NEXT});
    }

    /** Builds one case's branch and wires the guard's {@code slot} to it, accumulating open ends. */
    private void collectCaseBranch(Case<T> c, String guardId, int slot) {
        String[] start = new String[1];
        WorkflowStream<T> sub = new WorkflowStream<>(pipeline, id -> start[0] = id, enclosingJoinId);
        WorkflowStream<T> tail = c.body().apply(sub);
        if (start[0] == null) throw new IllegalArgumentException("case '" + c.name() + "' defines no steps");
        pipeline.wire(guardId, slot, start[0]);
        openNodes.addAll(tail.openNodes);
        openSlots.addAll(tail.openSlots);
    }

    /** Routes the step that was just added to a dedicated queue (for worker specialisation). */
    public WorkflowStream<T> onQueue(String queue) {
        if (lastStepId == null) {
            throw new IllegalStateException("onQueue() must directly follow step(), effect() or gate()");
        }
        pipeline.put(pipeline.get(lastStepId).withQueue(queue));
        pipeline.queues.add(queue);
        return this;
    }

    /** Sets the queue used by every subsequently defined step. */
    public WorkflowStream<T> defaultQueue(String queue) {
        pipeline.defaultQueue = Objects.requireNonNull(queue);
        return this;
    }

    /**
     * Sets how this workflow's steps are driven (default {@link ExecutionMode#DEFAULT}, i.e. the
     * server's configured default). The mode is part of the definition's content hash, so an
     * in-flight instance keeps the mode it started on.
     */
    public WorkflowStream<T> execution(ExecutionMode mode) {
        pipeline.executionMode = Objects.requireNonNull(mode, "mode");
        return this;
    }

    /**
     * Marks the step just added as a checkpoint: under {@link ExecutionMode#LOCAL_ASYNC} the worker
     * flushes its buffer to the server immediately after this step (committing it before running the
     * next), narrowing the crash-replay window for a step you don't want re-run. A no-op under
     * SERVER and LOCAL_SYNC, which already commit every step. Must directly follow a step.
     */
    public WorkflowStream<T> checkpoint() {
        if (lastStepId == null) {
            throw new IllegalStateException("checkpoint() must directly follow step(), effect() or gate()");
        }
        pipeline.checkpoints.add(lastStepId);
        return this;
    }

    public Blueprint<T> build() {
        if (consumed) throw new IllegalStateException("this workflow has already been built");
        consumed = true;
        if (pipeline.startNode == null) throw new IllegalStateException("workflow defines no steps");

        String endId = pipeline.nextId("end");
        pipeline.put(Node.end(endId, true, null));
        wireOpenEndsTo(endId);

        int version = WorkflowDefinition.contentVersion(pipeline.name, pipeline.startNode,
                pipeline.nodes.values(), pipeline.executionMode, pipeline.checkpoints);
        WorkflowDefinition def = new WorkflowDefinition(pipeline.name, version, pipeline.startNode,
                new LinkedHashMap<>(pipeline.nodes), pipeline.queues, pipeline.executionMode, pipeline.checkpoints);
        validate(def);
        return new Blueprint<>(def, pipeline.handlers, pipeline.codec);
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
            case TASK, SLEEP, JOIN, USER_TASK -> requireSuccessor(n);
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

    private void attach(String id) {
        if (openNodes.isEmpty()) {
            startSink.accept(id);
        } else {
            wireOpenEndsTo(id);
        }
        openNodes = new ArrayList<>(List.of(id));
        openSlots = new ArrayList<>(List.of(new int[]{NEXT}));
    }

    private void wireOpenEndsTo(String target) {
        for (int i = 0; i < openNodes.size(); i++) {
            pipeline.wire(openNodes.get(i), openSlots.get(i)[0], target);
        }
        openNodes = new ArrayList<>();
        openSlots = new ArrayList<>();
    }
}
