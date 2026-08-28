package dev.wiggle.client.dsl;

import dev.wiggle.core.ExecutionMode;
import dev.wiggle.core.RetryPolicy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

/**
 * A lazily-built workflow pipeline with a Stream-shaped API: intermediate operations
 * append nodes and return a stream, and the terminal {@link #build()} produces the
 * artifact. Nothing executes at definition time.
 *
 * <p>This class owns the graph's <em>shape</em> -- how nodes chain, branch, and rejoin -- while
 * {@link Pipeline} owns the graph's <em>storage</em> (nodes, handlers, ids, assembly). A stream
 * tracks its own "open ends" (nodes whose outgoing edge is not yet wired) and, for a branch,
 * the join it must fall back to; it asks the pipeline to create nodes and connect edges but
 * never touches the pipeline's collections.
 *
 * <p>One deliberate departure from {@code java.util.stream}: the context type does not
 * change from step to step. A workflow context is a durable document that survives
 * process restarts and is merged across parallel branches, so a single type for the
 * whole pipeline is what actually models the storage. {@code map} is therefore closer
 * to {@code UnaryOperator<T>} than to {@code Function<T,R>}.
 */
public final class WorkflowStream<T> {

    /** Which outgoing edge of a node an open end occupies. */
    private enum Edge { NEXT, ALT }

    private final Pipeline<T> pipeline;
    private final Consumer<String> startSink;
    /** Non-null when this stream is a fork branch: where a short-circuited branch must land. */
    private final String enclosingJoinId;
    private List<Edge> openSlots = new ArrayList<>();     // parallel arrays with openNodes
    private List<String> openNodes = new ArrayList<>();
    private String lastStepId;
    private boolean consumed;

    private WorkflowStream(Pipeline<T> pipeline, Consumer<String> startSink, String enclosingJoinId) {
        this.pipeline = pipeline;
        this.startSink = startSink;
        this.enclosingJoinId = enclosingJoinId;
    }

    static <T> WorkflowStream<T> root(Pipeline<T> pipeline) {
        return new WorkflowStream<>(pipeline, pipeline::startAt, null);
    }

    public WorkflowStream<T> step(String name) {
        return step(name, ctx -> ctx, (RetryPolicy) null, null);
    }

    /** A unit of work run on a worker: {@code fn}'s result becomes the new context. */
    public WorkflowStream<T> step(String name, Activity<T> fn) {
        return step(name, fn, (RetryPolicy) null, null);
    }

    /**
     * A unit of work run on a worker, with an explicit retry policy. Pass {@code retry} to
     * govern how a thrown step is retried (exponential backoff by default); a {@code null}
     * policy falls back to the workflow default.
     */
    public WorkflowStream<T> step(String name, Activity<T> fn, RetryPolicy retry) {
        return step(name, fn, retry, null);
    }


    public WorkflowStream<T> step(String name, String queue) {
        return step(name, ctx -> ctx, (RetryPolicy) null, queue);
    }

    /**
     * A unit of work pinned to a dedicated {@code queue}, so only workers polling that queue run it
     * (worker specialisation). A {@code null}/blank queue uses the workflow default.
     */
    public WorkflowStream<T> step(String name, Activity<T> fn, String queue) {
        return step(name, fn, (RetryPolicy) null, queue);
    }

    /** A unit of work with both an explicit retry policy and a dedicated queue. */
    public WorkflowStream<T> step(String name, Activity<T> fn, RetryPolicy retry, String queue) {
        Objects.requireNonNull(fn, "activity");
        String id = pipeline.addStep(name, fn, retry, queue);
        attach(id);
        lastStepId = id;
        return this;
    }

    /** Alias for {@link #step} that reads well when sequencing ("do this, then that"). */
    public WorkflowStream<T> then(String name, Activity<T> fn) {
        return step(name, fn);
    }

    /** Alias for {@link #step(String, Activity, RetryPolicy)}. */
    public WorkflowStream<T> then(String name, Activity<T> fn, RetryPolicy retry) {
        return step(name, fn, retry);
    }

    /** Alias for {@link #step(String, Activity, String)}. */
    public WorkflowStream<T> then(String name, Activity<T> fn, String queue) {
        return step(name, fn, queue);
    }

    /** Alias for {@link #step(String, Activity, RetryPolicy, String)}. */
    public WorkflowStream<T> then(String name, Activity<T> fn, RetryPolicy retry, String queue) {
        return step(name, fn, retry, queue);
    }

    public WorkflowStream<T> effect(String name) {
        return effect(name, ctx -> { }, (RetryPolicy) null, null);
    }

    /** Runs {@code fn} on a worker for its side effect only; the context is left unchanged. */
    public WorkflowStream<T> effect(String name, SideEffect<T> fn) {
        return effect(name, fn, (RetryPolicy) null, null);
    }

    /** {@link #effect} with an explicit retry policy; a {@code null} policy uses the workflow default. */
    public WorkflowStream<T> effect(String name, SideEffect<T> fn, RetryPolicy retry) {
        return effect(name, fn, retry, null);
    }

    /** {@link #effect} pinned to a dedicated {@code queue}; a {@code null}/blank queue uses the default. */
    public WorkflowStream<T> effect(String name, SideEffect<T> fn, String queue) {
        return effect(name, fn, (RetryPolicy) null, queue);
    }

    /** {@link #effect} with both an explicit retry policy and a dedicated queue. */
    public WorkflowStream<T> effect(String name, SideEffect<T> fn, RetryPolicy retry, String queue) {
        Objects.requireNonNull(fn, "side effect");
        String id = pipeline.addEffect(name, fn, retry, queue);
        attach(id);
        lastStepId = id;
        return this;
    }

    /** {@link #gate} whose guard uses the workflow's default retry policy. */
    public WorkflowStream<T> gate(String name, Predicate<T> test) {
        return gate(name, test, (RetryPolicy) null, null);
    }

    /** {@link #gate} with an explicit retry policy for the guard. */
    public WorkflowStream<T> gate(String name, Predicate<T> test, RetryPolicy retry) {
        return gate(name, test, retry, null);
    }

    /** {@link #gate} evaluated on a dedicated {@code queue}; a {@code null}/blank queue uses the default. */
    public WorkflowStream<T> gate(String name, Predicate<T> test, String queue) {
        return gate(name, test, (RetryPolicy) null, queue);
    }

    /**
     * A guard evaluated on a worker: the flow continues only while {@code test} holds. A false
     * result ends the instance successfully with the termination reason {@code "gated:<name>"}
     * -- the workflow equivalent of an empty stream, not an error.
     *
     * <p>Inside a fork branch the false path short-circuits to the enclosing join instead, so
     * the branch still arrives at the barrier and its siblings are not stranded. A {@code null}
     * retry policy uses the workflow default; a {@code null}/blank queue uses the default queue.
     */
    public WorkflowStream<T> gate(String name, Predicate<T> test, RetryPolicy retry, String queue) {
        Objects.requireNonNull(test, "predicate");
        String id = pipeline.addGuard(name, test, retry, queue);
        attach(id);

        if (enclosingJoinId != null) {
            pipeline.wireAlt(id, enclosingJoinId);
        } else {
            pipeline.wireAlt(id, pipeline.addEnd("gated:" + name));
        }

        openAt(id, Edge.NEXT);
        lastStepId = id;
        return this;
    }

    /** Server-side timer. No worker is occupied while the instance waits. */
    public WorkflowStream<T> sleep(Duration duration) {
        return sleep("sleep-" + duration.toMillis() + "ms", duration);
    }

    public WorkflowStream<T> sleep(String stepName, Duration duration) {
        if (duration.isNegative()) throw new IllegalArgumentException("sleep duration must not be negative");
        attach(pipeline.addSleep(stepName, duration.toMillis()));
        lastStepId = null;
        return this;
    }

    /**
     * Waits for the named signal from an external actor (a human, or another system) -- the flow
     * then continues down the following step. No worker is held while it waits. Deliver it via
     * {@code client.signal(instanceId, name, payload)}, the gRPC {@code SignalInstance} RPC, or
     * the dashboard ({@code POST /api/instances/{id}/signal/{name}}); the payload merges into the
     * context like a {@link #step}'s result.
     */
    public WorkflowStream<T> awaitSignal(String name) {
        return awaitSignal(name, null, null);
    }

    /**
     * A signal wait with a deadline. If the signal does not arrive within {@code timeout}, the
     * instance fails with a timeout error. Use
     * {@link #awaitSignal(String, Duration, UnaryOperator)} to escalate to a branch instead.
     */
    public WorkflowStream<T> awaitSignal(String name, Duration timeout) {
        return awaitSignal(name, timeout, null);
    }

    /**
     * A signal wait with a deadline and an escalation branch: if the signal does not arrive
     * within {@code timeout}, the {@code escalation} branch runs instead, then rejoins the flow
     * after the wait (exactly one of delivery / escalation happens).
     */
    public WorkflowStream<T> awaitSignal(String name, Duration timeout,
                                         UnaryOperator<WorkflowStream<T>> escalation) {
        if (timeout != null && timeout.isNegative()) throw new IllegalArgumentException("timeout must not be negative");
        if (timeout == null && escalation != null) throw new IllegalArgumentException("escalation needs a timeout");
        String id = pipeline.addSignal(name, timeout == null ? 0 : timeout.toMillis());
        attach(id);
        // The delivery path (NEXT) is the open end; the escalation branch hangs off ALT.
        openAt(id, Edge.NEXT);
        lastStepId = null;

        if (escalation != null) {
            Sub<T> esc = subStream(escalation, enclosingJoinId, "escalation branch of '" + name + "'");
            pipeline.wireAlt(id, esc.start());
            openNodes.addAll(esc.tail().openNodes);
            openSlots.addAll(esc.tail().openSlots);
        }
        return this;
    }

    /**
     * Runs the workflow named {@code workflow} as a child instance: it starts with this
     * instance's current context, and on completion its final context merges back here (a failed
     * or cancelled child fails this instance). The child must be registered on the server; its
     * latest version is used.
     */
    public WorkflowStream<T> subWorkflow(String name, String workflow) {
        Objects.requireNonNull(workflow, "workflow");
        attach(pipeline.addSubWorkflow(name, workflow));
        lastStepId = null;
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
        String forkId = pipeline.addFork();
        attach(forkId);

        String joinId = pipeline.addJoin(branches.length);
        List<String> starts = new ArrayList<>(branches.length);
        for (Branch<T> branch : branches) {
            starts.add(buildBranch(branch, joinId));
        }
        pipeline.setBranches(forkId, starts);

        openAt(joinId, Edge.NEXT);
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
        String forkId = pipeline.addDynFork(name, itemsKey, itemKey);
        attach(forkId);
        String joinId = pipeline.addJoin(0);   // 0 = dynamic width, carried in the join group
        String templateStart = buildBranch(Branch.of(name, body), joinId);
        pipeline.setBranches(forkId, List.of(templateStart));
        pipeline.wireNext(forkId, joinId);     // followed directly when the list is empty
        openAt(joinId, Edge.NEXT);
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
        Sub<T> body0 = subStream(body, enclosingJoinId, "doWhile body");
        String condId = pipeline.addGuard(conditionName, condition, null, null);

        // Enter at the body; body tail feeds the condition; true loops, false continues onward.
        if (openNodes.isEmpty()) startSink.accept(body0.start()); else wireOpenEndsTo(body0.start());
        body0.tail().wireOpenEndsTo(condId);
        pipeline.wireNext(condId, body0.start());
        openAt(condId, Edge.ALT);
        lastStepId = null;
        return this;
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
        Collections.addAll(all, cases);
        boolean hasDefault = validateChoose(all);
        int guards = hasDefault ? all.size() - 1 : all.size();

        // Lay down the guard predicates first, so each false path can point at the next guard.
        String[] guardIds = new String[guards];
        for (int i = 0; i < guards; i++) {
            Case<T> c = all.get(i);
            guardIds[i] = pipeline.addGuard(c.name(), c.guard(), null, null);
        }

        // Enter at the first guard, then take over the open-end bookkeeping ourselves.
        attach(guardIds[0]);
        openNodes = new ArrayList<>();
        openSlots = new ArrayList<>();

        // Chain each guard's false path to the next guard.
        for (int i = 0; i < guards - 1; i++) {
            pipeline.wireAlt(guardIds[i], guardIds[i + 1]);
        }

        // Each guard's true path runs its branch; the branch's open ends become the choose's.
        for (int i = 0; i < guards; i++) {
            collectCaseBranch(all.get(i), guardIds[i], Edge.NEXT);
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

    /** The last false path: a default branch, or an open end that skips the choose entirely. */
    private void wireChooseFallthrough(List<Case<T>> cases, String[] guardIds, boolean hasDefault) {
        String lastGuard = guardIds[guardIds.length - 1];
        if (hasDefault) {
            collectCaseBranch(cases.get(cases.size() - 1), lastGuard, Edge.ALT);
        } else {
            openNodes.add(lastGuard);
            openSlots.add(Edge.ALT);
        }
    }

    /** Builds one case's branch and wires the guard's {@code edge} to it, accumulating open ends. */
    private void collectCaseBranch(Case<T> c, String guardId, Edge edge) {
        Sub<T> branch = subStream(c.body(), enclosingJoinId, "case '" + c.name() + "'");
        wire(guardId, edge, branch.start());
        openNodes.addAll(branch.tail().openNodes);
        openSlots.addAll(branch.tail().openSlots);
    }

    /** Builds one fork branch as a sub-stream wired to the join; returns its start node id. */
    private String buildBranch(Branch<T> branch, String joinId) {
        Sub<T> built = subStream(branch.body(), joinId, "branch '" + branch.name() + "'");
        built.tail().wireOpenEndsTo(joinId);
        return built.start();
    }

    /** A built nested sub-stream: its first node ({@code start}) and its open-ended {@code tail}. */
    private record Sub<U>(String start, WorkflowStream<U> tail) {}

    /**
     * Builds a nested sub-stream from {@code body} over the same pipeline, falling back to
     * {@code joinId} for any short-circuit. Throws with {@code what} if the body defines no steps.
     */
    private Sub<T> subStream(UnaryOperator<WorkflowStream<T>> body, String joinId, String what) {
        String[] start = new String[1];
        WorkflowStream<T> sub = new WorkflowStream<>(pipeline, id -> start[0] = id, joinId);
        WorkflowStream<T> tail = body.apply(sub);
        if (start[0] == null) throw new IllegalArgumentException(what + " defines no steps");
        return new Sub<>(start[0], tail);
    }

    /** Sets the queue used by every subsequently defined step (per-step {@code queue} overrides it). */
    public WorkflowStream<T> defaultQueue(String queue) {
        pipeline.defaultQueue(queue);
        return this;
    }

    /**
     * Sets how this workflow's steps are driven (default {@link ExecutionMode#DEFAULT}, i.e. the
     * server's configured default). The mode is part of the definition's content hash, so an
     * in-flight instance keeps the mode it started on.
     */
    public WorkflowStream<T> execution(ExecutionMode mode) {
        pipeline.executionMode(mode);
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
        pipeline.markCheckpoint(lastStepId);
        return this;
    }

    public Blueprint<T> build() {
        if (consumed) throw new IllegalStateException("this workflow has already been built");
        consumed = true;
        wireOpenEndsTo(pipeline.addEnd(null));
        return pipeline.build();
    }

    // ------------------------------------------------------ open-end bookkeeping

    /** Appends {@code id}, wiring any pending open ends into it, then makes it the sole open end. */
    private void attach(String id) {
        if (openNodes.isEmpty()) {
            startSink.accept(id);
        } else {
            wireOpenEndsTo(id);
        }
        openAt(id, Edge.NEXT);
    }

    /** Replaces the open-end set with the single edge {@code edge} of node {@code id}. */
    private void openAt(String id, Edge edge) {
        openNodes = new ArrayList<>(List.of(id));
        openSlots = new ArrayList<>(List.of(edge));
    }

    private void wireOpenEndsTo(String target) {
        for (int i = 0; i < openNodes.size(); i++) {
            wire(openNodes.get(i), openSlots.get(i), target);
        }
        openNodes = new ArrayList<>();
        openSlots = new ArrayList<>();
    }

    private void wire(String from, Edge edge, String target) {
        switch (edge) {
            case NEXT -> pipeline.wireNext(from, target);
            case ALT -> pipeline.wireAlt(from, target);
        }
    }
}
