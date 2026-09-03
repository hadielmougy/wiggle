package com.wiggle.client.dsl;

import com.wiggle.core.ExecutionMode;
import com.wiggle.core.RetryPolicy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

/**
 * A lazily-built workflow <em>topology</em> with a Stream-shaped API: intermediate operations append
 * nodes and return the stream, and the terminal {@link #build()} produces the {@link Blueprint}.
 * Nothing executes at definition time, and no step logic lives here -- every {@code step}/{@code
 * gate}/{@code effect}/{@code combine} is just a named node. The implementations are bound
 * separately on a worker via {@link com.wiggle.client.worker.Handlers @Handlers} classes, matched to
 * the graph by name; a method's signature there defines its input/output types (types may change
 * from step to step, like {@code Stream.map}).
 *
 * <p>This class owns the graph's <em>shape</em> -- how nodes chain, branch, and rejoin -- while
 * {@link Pipeline} owns its <em>storage</em>. A stream tracks its own "open ends" and, for a branch,
 * the join it must fall back to.
 */
public final class WorkflowStream {

    /** Which outgoing edge of a node an open end occupies. */
    private enum Edge { NEXT, ALT }

    private final Pipeline pipeline;
    private final Consumer<String> startSink;
    /** Non-null when this stream is a fork branch: where a short-circuited branch must land. */
    private final String enclosingJoinId;
    private List<Edge> openSlots = new ArrayList<>();     // parallel arrays with openNodes
    private List<String> openNodes = new ArrayList<>();
    private String lastStepId;
    /** True between {@link #fork} and its {@link ForkStage} choosing a combine; a build/nest with a
     *  fork still pending is a forgotten {@code combine()} and is rejected. */
    private boolean forkPending;
    private boolean consumed;

    private WorkflowStream(Pipeline pipeline, Consumer<String> startSink, String enclosingJoinId) {
        this.pipeline = pipeline;
        this.startSink = startSink;
        this.enclosingJoinId = enclosingJoinId;
    }

    static WorkflowStream root(Pipeline pipeline) {
        return new WorkflowStream(pipeline, pipeline::startAt, null);
    }

    // ------------------------------------------------------ step / effect (both TASK nodes)

    /** A task step; its handler is bound on the worker by {@code name}. */
    public WorkflowStream step(String name) {
        return wireStep(pipeline.addTask(name, null, null));
    }

    /** A task step pinned to a dedicated {@code queue} (worker specialisation). */
    public WorkflowStream step(String name, String queue) {
        return wireStep(pipeline.addTask(name, null, queue));
    }

    /** A task step with an explicit retry policy. */
    public WorkflowStream step(String name, RetryPolicy retry) {
        return wireStep(pipeline.addTask(name, retry, null));
    }

    /** A task step with both an explicit retry policy and a dedicated queue. */
    public WorkflowStream step(String name, RetryPolicy retry, String queue) {
        return wireStep(pipeline.addTask(name, retry, queue));
    }

    /** Alias for {@link #step(String)} that reads well when sequencing ("do this, then that"). */
    public WorkflowStream then(String name) {
        return step(name);
    }

    /** An effect step (its handler returns void, leaving the context unchanged). Topologically a task. */
    public WorkflowStream effect(String name) {
        return wireStep(pipeline.addTask(name, null, null));
    }

    /** {@link #effect} pinned to a dedicated {@code queue}. */
    public WorkflowStream effect(String name, String queue) {
        return wireStep(pipeline.addTask(name, null, queue));
    }

    /** {@link #effect} with an explicit retry policy. */
    public WorkflowStream effect(String name, RetryPolicy retry) {
        return wireStep(pipeline.addTask(name, retry, null));
    }

    /** {@link #effect} with both an explicit retry policy and a dedicated queue. */
    public WorkflowStream effect(String name, RetryPolicy retry, String queue) {
        return wireStep(pipeline.addTask(name, retry, queue));
    }

    /** Shared wiring for a task node: attach it to the open edge and mark it the last step. */
    private WorkflowStream wireStep(String id) {
        attach(id);
        lastStepId = id;
        return this;
    }

    // ------------------------------------------------------ gate

    /** A guard; its boolean handler is bound on the worker by {@code name}. */
    public WorkflowStream gate(String name) {
        return wireGate(pipeline.addGuard(name, null, null), name);
    }

    /** {@link #gate} pinned to a dedicated {@code queue}. */
    public WorkflowStream gate(String name, String queue) {
        return wireGate(pipeline.addGuard(name, null, queue), name);
    }

    /** {@link #gate} with an explicit retry policy for the guard. */
    public WorkflowStream gate(String name, RetryPolicy retry) {
        return wireGate(pipeline.addGuard(name, retry, null), name);
    }

    /** {@link #gate} with both an explicit retry policy and a dedicated queue. */
    public WorkflowStream gate(String name, RetryPolicy retry, String queue) {
        return wireGate(pipeline.addGuard(name, retry, queue), name);
    }

    /** Shared wiring for a guard node: attach it, route its false edge (to the enclosing join, else a
     *  {@code gated:<name>} end), and open the true edge. */
    private WorkflowStream wireGate(String id, String name) {
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

    // ------------------------------------------------------ sleep / signal / sub-workflow

    /** Server-side timer. No worker is occupied while the instance waits. */
    public WorkflowStream sleep(Duration duration) {
        return sleep("sleep-" + duration.toMillis() + "ms", duration);
    }

    public WorkflowStream sleep(String stepName, Duration duration) {
        if (duration.isNegative()) throw new IllegalArgumentException("sleep duration must not be negative");
        attach(pipeline.addSleep(stepName, duration.toMillis()));
        lastStepId = null;
        return this;
    }

    /**
     * Waits for the named signal from an external actor -- the flow then continues down the following
     * step. No worker is held while it waits; the payload merges into the context like a step result.
     */
    public WorkflowStream awaitSignal(String name) {
        return awaitSignal(name, null, null);
    }

    /** A signal wait with a deadline; on timeout the instance fails. */
    public WorkflowStream awaitSignal(String name, Duration timeout) {
        return awaitSignal(name, timeout, null);
    }

    /**
     * A signal wait with a deadline and an escalation branch: if the signal does not arrive within
     * {@code timeout}, the {@code escalation} branch runs instead, then rejoins the flow after the
     * wait (exactly one of delivery / escalation happens).
     */
    public WorkflowStream awaitSignal(String name, Duration timeout, UnaryOperator<WorkflowStream> escalation) {
        if (timeout != null && timeout.isNegative()) throw new IllegalArgumentException("timeout must not be negative");
        if (timeout == null && escalation != null) throw new IllegalArgumentException("escalation needs a timeout");
        String id = pipeline.addSignal(name, timeout == null ? 0 : timeout.toMillis());
        attach(id);
        openAt(id, Edge.NEXT);
        lastStepId = null;

        if (escalation != null) {
            Sub esc = subStream(escalation, enclosingJoinId, "escalation branch of '" + name + "'");
            pipeline.wireAlt(id, esc.start());
            openNodes.addAll(esc.tail().openNodes);
            openSlots.addAll(esc.tail().openSlots);
        }
        return this;
    }

    /**
     * Runs the workflow named {@code workflow} as a child instance: it starts with this instance's
     * current context, and on completion its final context merges back here. The child must be
     * registered on the server; its latest version is used.
     */
    public WorkflowStream subWorkflow(String name, String workflow) {
        java.util.Objects.requireNonNull(workflow, "workflow");
        attach(pipeline.addSubWorkflow(name, workflow));
        lastStepId = null;
        return this;
    }

    // ------------------------------------------------------ fork / combine

    /**
     * Fans out into parallel branches that run independently (possibly on different workers) and all
     * join before the flow continues. Each branch runs on its <em>own isolated copy</em> of the
     * context -- a branch's writes are invisible to its siblings and never touch the shared context
     * -- so there is no implicit merge. The returned {@link ForkStage} requires a
     * {@link ForkStage#combine combine}, whose worker handler receives every branch's result keyed by
     * {@link Branch#name() name}. The fork leaves the stream with no open end, so a forgotten combine
     * fails at {@code build()}.
     */
    public ForkStage fork(Branch... branches) {
        if (branches.length < 2) throw new IllegalArgumentException("fork needs at least two branches");
        forkPending = true;   // cleared only when the returned ForkStage's combine() runs
        return new ForkStage(this, List.of(branches));
    }

    /**
     * Builds an isolated fork with a mandatory combine node. Each branch is an independent
     * sub-pipeline; the combine node carries the arm names so a worker's combine handler (or the
     * default union) can key each branch's result by name. Reopens the stream at the combine node.
     */
    void buildForkCombine(List<Branch> branches, String combineName) {
        forkPending = false;
        String forkId = pipeline.addFork();
        attach(forkId);

        String joinId = pipeline.addJoin(branches.size());
        List<String> starts = new ArrayList<>(branches.size());
        List<String> names = new ArrayList<>(branches.size());
        for (Branch branch : branches) {
            starts.add(buildBranch(branch, joinId));
            names.add(branch.name());
        }
        pipeline.setBranches(forkId, starts);

        String combineId = pipeline.addCombine(combineName, names, null, null);
        pipeline.wireNext(joinId, combineId);   // JOIN -> combine
        openAt(combineId, Edge.NEXT);           // reopen the stream after the combine node
        lastStepId = combineId;
    }

    Pipeline pipeline() {
        return pipeline;
    }

    /**
     * Runtime fan-out: when the instance reaches this node, the engine reads the list stored in the
     * context under {@code itemsKey} and spawns one parallel branch per element, each running
     * {@code body} with its element injected under {@code itemKey} (and its position under
     * {@code itemKey + "Index"}). All branches join before the flow continues; an empty list skips
     * straight through. Branch writes merge into the shared context (last write wins), so per-element
     * results belong under per-element keys (use the index).
     */
    public WorkflowStream forkEach(String name, String itemsKey, String itemKey, UnaryOperator<WorkflowStream> body) {
        java.util.Objects.requireNonNull(itemsKey, "itemsKey");
        java.util.Objects.requireNonNull(itemKey, "itemKey");
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
     * A do-while loop: runs {@code body} once, then evaluates the guard named {@code conditionName}
     * on a worker; while it holds, the body runs again. Compiles to a plain cycle in the graph, so it
     * works identically under every execution mode.
     */
    public WorkflowStream doWhile(String conditionName, UnaryOperator<WorkflowStream> body) {
        Sub body0 = subStream(body, enclosingJoinId, "doWhile body");
        String condId = pipeline.addGuard(conditionName, null, null);

        if (openNodes.isEmpty()) startSink.accept(body0.start()); else wireOpenEndsTo(body0.start());
        body0.tail().wireOpenEndsTo(condId);
        pipeline.wireNext(condId, body0.start());
        openAt(condId, Edge.ALT);
        lastStepId = null;
        return this;
    }

    /**
     * Exclusive choice -- a switch/case over the context. Each case's guard is evaluated in order and
     * the first one to hold runs its branch; the rest are skipped. If no guard matches, control passes
     * to an {@link Case#otherwise} branch when one is given, otherwise straight to the step after
     * {@code choose}. Exactly one branch ever runs.
     */
    public WorkflowStream choose(Case... cases) {
        List<Case> all = new ArrayList<>(cases.length);
        Collections.addAll(all, cases);
        boolean hasDefault = validateChoose(all);
        int guards = hasDefault ? all.size() - 1 : all.size();

        String[] guardIds = new String[guards];
        for (int i = 0; i < guards; i++) {
            guardIds[i] = pipeline.addGuard(all.get(i).name(), null, null);
        }

        attach(guardIds[0]);
        openNodes = new ArrayList<>();
        openSlots = new ArrayList<>();

        for (int i = 0; i < guards - 1; i++) {
            pipeline.wireAlt(guardIds[i], guardIds[i + 1]);
        }
        for (int i = 0; i < guards; i++) {
            collectCaseBranch(all.get(i), guardIds[i], Edge.NEXT);
        }

        wireChooseFallthrough(all, guardIds, hasDefault);
        lastStepId = null;
        return this;
    }

    private boolean validateChoose(List<Case> cases) {
        if (cases.isEmpty()) throw new IllegalArgumentException("choose needs at least one case");
        for (int i = 0; i < cases.size() - 1; i++) {
            if (!cases.get(i).guarded()) throw new IllegalArgumentException("otherwise() must be the last case");
        }
        boolean hasDefault = !cases.get(cases.size() - 1).guarded();
        if (hasDefault && cases.size() == 1) {
            throw new IllegalArgumentException("choose needs at least one guarded case");
        }
        return hasDefault;
    }

    /** The last false path: a default branch, or an open end that skips the choose entirely. */
    private void wireChooseFallthrough(List<Case> cases, String[] guardIds, boolean hasDefault) {
        String lastGuard = guardIds[guardIds.length - 1];
        if (hasDefault) {
            collectCaseBranch(cases.get(cases.size() - 1), lastGuard, Edge.ALT);
        } else {
            openNodes.add(lastGuard);
            openSlots.add(Edge.ALT);
        }
    }

    /** Builds one case's branch and wires the guard's {@code edge} to it, accumulating open ends. */
    private void collectCaseBranch(Case c, String guardId, Edge edge) {
        Sub branch = subStream(c.body(), enclosingJoinId, "case '" + c.name() + "'");
        wire(guardId, edge, branch.start());
        openNodes.addAll(branch.tail().openNodes);
        openSlots.addAll(branch.tail().openSlots);
    }

    /** Builds one fork branch as a sub-stream wired to the join; returns its start node id. */
    private String buildBranch(Branch branch, String joinId) {
        Sub built = subStream(branch.body(), joinId, "branch '" + branch.name() + "'");
        built.tail().wireOpenEndsTo(joinId);
        return built.start();
    }

    /** A built nested sub-stream: its first node ({@code start}) and its open-ended {@code tail}. */
    private record Sub(String start, WorkflowStream tail) {}

    /**
     * Builds a nested sub-stream from {@code body} over the same pipeline, falling back to
     * {@code joinId} for any short-circuit. Throws with {@code what} if the body defines no steps.
     */
    private Sub subStream(UnaryOperator<WorkflowStream> body, String joinId, String what) {
        String[] start = new String[1];
        WorkflowStream sub = new WorkflowStream(pipeline, id -> start[0] = id, joinId);
        WorkflowStream tail = body.apply(sub);
        if (start[0] == null) throw new IllegalArgumentException(what + " defines no steps");
        if (tail.forkPending) throw new IllegalStateException(what + " has a fork(...) with no combine()");
        return new Sub(start[0], tail);
    }

    // ------------------------------------------------------ workflow-level settings / terminal

    /** Sets the queue used by every subsequently defined step (per-step {@code queue} overrides it). */
    public WorkflowStream defaultQueue(String queue) {
        pipeline.defaultQueue(queue);
        return this;
    }

    /**
     * Sets how this workflow's steps are driven (default {@link ExecutionMode#DEFAULT}). The mode is
     * part of the definition's content hash, so an in-flight instance keeps the mode it started on.
     */
    public WorkflowStream execution(ExecutionMode mode) {
        pipeline.executionMode(mode);
        return this;
    }

    /**
     * Marks the step just added as a checkpoint: under {@link ExecutionMode#LOCAL_ASYNC} the worker
     * flushes its buffer to the server immediately after this step. A no-op under SERVER and
     * LOCAL_SYNC. Must directly follow a step.
     */
    public WorkflowStream checkpoint() {
        if (lastStepId == null) {
            throw new IllegalStateException("checkpoint() must directly follow step(), effect() or gate()");
        }
        pipeline.markCheckpoint(lastStepId);
        return this;
    }

    public Blueprint build() {
        if (consumed) throw new IllegalStateException("this workflow has already been built");
        if (forkPending) throw new IllegalStateException(
                "a fork(...) has no merge: follow it with combine(...) before build()");
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
