package com.wiggle.client.dsl;

import com.wiggle.core.ExecutionMode;
import com.wiggle.core.RetryPolicy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

/**
 * A lazily-built workflow pipeline with a Stream-shaped API: intermediate operations append nodes
 * and return a stream, and the terminal {@link #build()} produces the artifact. Nothing executes at
 * definition time.
 *
 * <p>This class owns the graph's <em>shape</em> -- how nodes chain, branch, and rejoin -- while
 * {@link Pipeline} owns the graph's <em>storage</em> (nodes, handlers, ids, assembly). A stream
 * tracks its own "open ends" (nodes whose outgoing edge is not yet wired) and, for a branch, the
 * join it must fall back to; it asks the pipeline to create nodes and connect edges but never
 * touches the pipeline's collections.
 *
 * <p>Like {@code java.util.stream}, a step is a {@code map}: it takes an input type and returns a
 * possibly different one. The workflow carries no single context type; each {@code step}/{@code
 * gate}/{@code effect} declares its input {@code Class} so the engine can rebuild it from the JSON
 * persisted (and merged across branches) between steps.
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
    /** True between {@link #fork} and its {@link ForkStage} choosing a merge; a build/nest with a
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

    // ------------------------------------------------------ step

    /**
     * A name-only step: declares the topology node (default queue), with <em>no</em> inline handler.
     * The step's logic is bound on the worker by name ({@code handle}/{@code registerHandlers}); a
     * worker that claims it without a bound handler fails it fast (no silent no-op).
     */
    public WorkflowStream step(String name) {
        return wireStep(pipeline.addTaskNameOnly(name, null, null));
    }

    /** A name-only step pinned to a dedicated {@code queue}; the handler is bound on the worker. */
    public WorkflowStream step(String name, String queue) {
        return wireStep(pipeline.addTaskNameOnly(name, null, queue));
    }

    /** A step in the spirit of {@code Stream.map}: {@code fn} takes {@code in} and returns the next
     *  context (a possibly different type), which becomes the new context. */
    public <I, O> WorkflowStream step(String name, Class<I> in, Step<I, O> fn) {
        return step(name, in, fn, null, null);
    }

    /** {@link #step(String, Class, Step)} with an explicit retry policy. */
    public <I, O> WorkflowStream step(String name, Class<I> in, Step<I, O> fn, RetryPolicy retry) {
        return step(name, in, fn, retry, null);
    }

    /** {@link #step(String, Class, Step)} pinned to a dedicated {@code queue}. */
    public <I, O> WorkflowStream step(String name, Class<I> in, Step<I, O> fn, String queue) {
        return step(name, in, fn, null, queue);
    }

    /** {@link #step(String, Class, Step)} with both an explicit retry policy and a dedicated queue. */
    public <I, O> WorkflowStream step(String name, Class<I> in, Step<I, O> fn, RetryPolicy retry, String queue) {
        Objects.requireNonNull(fn, "step");
        return wireStep(pipeline.addStep(name, in, fn, retry, queue));
    }

    /** Alias for {@link #step(String, Class, Step)} that reads well when sequencing. */
    public <I, O> WorkflowStream then(String name, Class<I> in, Step<I, O> fn) {
        return step(name, in, fn);
    }

    /** Alias for {@link #step(String, Class, Step, RetryPolicy)}. */
    public <I, O> WorkflowStream then(String name, Class<I> in, Step<I, O> fn, RetryPolicy retry) {
        return step(name, in, fn, retry);
    }

    /** Alias for {@link #step(String, Class, Step, String)}. */
    public <I, O> WorkflowStream then(String name, Class<I> in, Step<I, O> fn, String queue) {
        return step(name, in, fn, queue);
    }

    // --- JSON-context conveniences: the input is the raw JSON document (Map), no Class needed ---

    /** A step over the raw JSON context (a {@code Map}); {@code fn} returns the next context. */
    public <O> WorkflowStream step(String name, Step<Map<String, Object>, O> fn) {
        return wireStep(pipeline.addStep(name, null, fn, null, null));
    }

    /** {@link #step(String, Step)} with an explicit retry policy. */
    public <O> WorkflowStream step(String name, Step<Map<String, Object>, O> fn, RetryPolicy retry) {
        return wireStep(pipeline.addStep(name, null, fn, retry, null));
    }

    /** {@link #step(String, Step)} pinned to a dedicated {@code queue}. */
    public <O> WorkflowStream step(String name, Step<Map<String, Object>, O> fn, String queue) {
        return wireStep(pipeline.addStep(name, null, fn, null, queue));
    }

    /** {@link #step(String, Step)} with both an explicit retry policy and a dedicated queue. */
    public <O> WorkflowStream step(String name, Step<Map<String, Object>, O> fn, RetryPolicy retry, String queue) {
        return wireStep(pipeline.addStep(name, null, fn, retry, queue));
    }

    /** Alias for {@link #step(String, Step)}. */
    public <O> WorkflowStream then(String name, Step<Map<String, Object>, O> fn) {
        return step(name, fn);
    }

    /** Shared wiring for a worker step/effect node: attach it to the open edge and mark it the last step. */
    private WorkflowStream wireStep(String id) {
        attach(id);
        lastStepId = id;
        return this;
    }

    // ------------------------------------------------------ effect

    /** A name-only effect: declares the topology node, handler bound on the worker by name. */
    public WorkflowStream effect(String name) {
        return wireStep(pipeline.addTaskNameOnly(name, null, null));
    }

    /** An effect over the raw JSON context (a {@code Map}); the context is left unchanged. */
    public WorkflowStream effect(String name, SideEffect<Map<String, Object>> fn) {
        return effect(name, null, fn, null, null);
    }

    /** {@link #effect(String, SideEffect)} with an explicit retry policy. */
    public WorkflowStream effect(String name, SideEffect<Map<String, Object>> fn, RetryPolicy retry) {
        return effect(name, null, fn, retry, null);
    }

    /** {@link #effect(String, SideEffect)} pinned to a dedicated {@code queue}. */
    public WorkflowStream effect(String name, SideEffect<Map<String, Object>> fn, String queue) {
        return effect(name, null, fn, null, queue);
    }

    /** {@link #effect(String, SideEffect)} with both an explicit retry policy and a dedicated queue. */
    public WorkflowStream effect(String name, SideEffect<Map<String, Object>> fn, RetryPolicy retry, String queue) {
        return effect(name, null, fn, retry, queue);
    }

    /** Runs {@code fn} on a worker for its side effect only; the context is left unchanged. */
    public <I> WorkflowStream effect(String name, Class<I> in, SideEffect<I> fn) {
        return effect(name, in, fn, null, null);
    }

    /** {@link #effect} with an explicit retry policy. */
    public <I> WorkflowStream effect(String name, Class<I> in, SideEffect<I> fn, RetryPolicy retry) {
        return effect(name, in, fn, retry, null);
    }

    /** {@link #effect} pinned to a dedicated {@code queue}. */
    public <I> WorkflowStream effect(String name, Class<I> in, SideEffect<I> fn, String queue) {
        return effect(name, in, fn, null, queue);
    }

    /** {@link #effect} with both an explicit retry policy and a dedicated queue. */
    public <I> WorkflowStream effect(String name, Class<I> in, SideEffect<I> fn, RetryPolicy retry, String queue) {
        Objects.requireNonNull(fn, "side effect");
        return wireStep(pipeline.addEffect(name, in, fn, retry, queue));
    }

    // ------------------------------------------------------ gate

    /** A name-only guard pinned to a dedicated {@code queue}; the predicate is bound on the worker. */
    public WorkflowStream gate(String name, String queue) {
        return wireGate(pipeline.addGuardNameOnly(name, null, queue), name);
    }

    /** A name-only guard with an explicit retry policy and a dedicated {@code queue}. */
    public WorkflowStream gate(String name, RetryPolicy retry, String queue) {
        return wireGate(pipeline.addGuardNameOnly(name, retry, queue), name);
    }

    /**
     * A guard evaluated on a worker: the flow continues only while {@code test} holds. A false
     * result ends the instance successfully with the termination reason {@code "gated:<name>"} --
     * the workflow equivalent of an empty stream, not an error.
     *
     * <p>Inside a fork branch the false path short-circuits to the enclosing join instead, so the
     * branch still arrives at the barrier and its siblings are not stranded.
     */
    public <I> WorkflowStream gate(String name, Class<I> in, Predicate<I> test) {
        return gate(name, in, test, null, null);
    }

    /** A gate over the raw JSON context (a {@code Map}). */
    public WorkflowStream gate(String name, Predicate<Map<String, Object>> test) {
        return gate(name, null, test, null, null);
    }

    /** {@link #gate(String, Predicate)} with an explicit retry policy for the guard. */
    public WorkflowStream gate(String name, Predicate<Map<String, Object>> test, RetryPolicy retry) {
        return gate(name, null, test, retry, null);
    }

    /** {@link #gate(String, Predicate)} evaluated on a dedicated {@code queue}. */
    public WorkflowStream gate(String name, Predicate<Map<String, Object>> test, String queue) {
        return gate(name, null, test, null, queue);
    }

    /** {@link #gate(String, Predicate)} with both an explicit retry policy and a dedicated queue. */
    public WorkflowStream gate(String name, Predicate<Map<String, Object>> test, RetryPolicy retry, String queue) {
        return gate(name, null, test, retry, queue);
    }

    /** {@link #gate} with an explicit retry policy for the guard. */
    public <I> WorkflowStream gate(String name, Class<I> in, Predicate<I> test, RetryPolicy retry) {
        return gate(name, in, test, retry, null);
    }

    /** {@link #gate} evaluated on a dedicated {@code queue}. */
    public <I> WorkflowStream gate(String name, Class<I> in, Predicate<I> test, String queue) {
        return gate(name, in, test, null, queue);
    }

    /** {@link #gate} with both an explicit retry policy and a dedicated queue. */
    public <I> WorkflowStream gate(String name, Class<I> in, Predicate<I> test, RetryPolicy retry, String queue) {
        Objects.requireNonNull(test, "predicate");
        return wireGate(pipeline.addGuard(name, in, test, retry, queue), name);
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
     * Waits for the named signal from an external actor -- the flow then continues down the
     * following step. No worker is held while it waits. Deliver it via
     * {@code client.signal(instanceId, name, payload)}, the gRPC {@code SignalInstance} RPC, or the
     * dashboard; the payload merges into the context like a {@link #step}'s result.
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
        // The delivery path (NEXT) is the open end; the escalation branch hangs off ALT.
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
     * current context, and on completion its final context merges back here (a failed or cancelled
     * child fails this instance). The child must be registered on the server; its latest version is
     * used.
     */
    public WorkflowStream subWorkflow(String name, String workflow) {
        Objects.requireNonNull(workflow, "workflow");
        attach(pipeline.addSubWorkflow(name, workflow));
        lastStepId = null;
        return this;
    }

    // ------------------------------------------------------ fork / combine

    /**
     * Fans out into parallel branches that run independently (possibly on different workers) and all
     * join before the flow continues. Each branch runs on its <em>own isolated copy</em> of the
     * context -- a branch's writes are invisible to its siblings and never touch the shared context
     * -- so there is no implicit merge. The returned {@link ForkStage} therefore requires a
     * {@link ForkStage#combine combine}: it receives every branch's result keyed by
     * {@link Branch#name() name} and returns the fields to merge back. The fork leaves the stream
     * with no open end, so a forgotten combine fails at {@code build()}.
     */
    public ForkStage fork(Branch... branches) {
        if (branches.length < 2) throw new IllegalArgumentException("fork needs at least two branches");
        forkPending = true;   // cleared only when the returned ForkStage's combine() runs
        return new ForkStage(this, List.of(branches));
    }

    // --- package-private hook for ForkStage (same package) ---

    /**
     * Builds a fork whose branches are isolated and rejoined by a mandatory {@code combine}: each
     * branch is an independent sub-pipeline; at the join every branch's accumulated changes are
     * handed to {@code combine} keyed by branch name, and its return value is merged into the
     * context. Reopens the stream at the combine node.
     */
    void buildForkCombine(List<Branch> branches, String combineName, Aggregator combine) {
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

        // The aggregator carries the arm names on its node too, so the engine can key each isolated
        // branch's result by name when it assembles the combine input at the join.
        String aggId = pipeline.addAggregator(combineName, names, combine, null, null);
        pipeline.wireNext(joinId, aggId);   // JOIN -> combine
        openAt(aggId, Edge.NEXT);           // reopen the stream after the combine node
        lastStepId = aggId;
    }

    Pipeline pipeline() {
        return pipeline;
    }

    /**
     * Runtime fan-out: when the instance reaches this node, the engine reads the list stored in the
     * context under {@code itemsKey} and spawns one parallel branch per element, each running
     * {@code body} with its element injected into the context under {@code itemKey} (and its
     * position under {@code itemKey + "Index"}). All branches join before the flow continues; an
     * empty or missing list skips straight through.
     *
     * <p>Branch writes merge into the shared context: last write to the same key wins, so
     * per-element results belong under per-element keys (use the index).
     */
    public WorkflowStream forkEach(String name, String itemsKey, String itemKey, UnaryOperator<WorkflowStream> body) {
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
     * A do-while loop: runs {@code body} once, then evaluates {@code condition} (over input type
     * {@code in}) on a worker; while it holds, the body runs again. Compiles to a plain cycle in the
     * graph, so it works identically under every execution mode.
     */
    /** A do-while loop whose condition reads the raw JSON context (a {@code Map}). */
    public WorkflowStream doWhile(String conditionName, Predicate<Map<String, Object>> condition,
                                  UnaryOperator<WorkflowStream> body) {
        return doWhile(conditionName, null, condition, body);
    }

    public <I> WorkflowStream doWhile(String conditionName, Class<I> in, Predicate<I> condition,
                                      UnaryOperator<WorkflowStream> body) {
        Objects.requireNonNull(condition, "condition");
        Sub body0 = subStream(body, enclosingJoinId, "doWhile body");
        String condId = pipeline.addGuard(conditionName, in, condition, null, null);

        // Enter at the body; body tail feeds the condition; true loops, false continues onward.
        if (openNodes.isEmpty()) startSink.accept(body0.start()); else wireOpenEndsTo(body0.start());
        body0.tail().wireOpenEndsTo(condId);
        pipeline.wireNext(condId, body0.start());
        openAt(condId, Edge.ALT);
        lastStepId = null;
        return this;
    }

    /**
     * Exclusive choice -- a switch/case over the context. Each case's guard is evaluated in order and
     * the first one to hold runs its branch; the rest are skipped. If no guard matches, control
     * passes to an {@link Case#otherwise} branch when one is given, otherwise straight to the step
     * after {@code choose}. Exactly one branch ever runs.
     */
    public WorkflowStream choose(Case... cases) {
        List<Case> all = new ArrayList<>(cases.length);
        Collections.addAll(all, cases);
        boolean hasDefault = validateChoose(all);
        int guards = hasDefault ? all.size() - 1 : all.size();

        // Lay down the guard predicates first, so each false path can point at the next guard.
        String[] guardIds = new String[guards];
        for (int i = 0; i < guards; i++) {
            Case c = all.get(i);
            guardIds[i] = addCaseGuard(c);
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

    @SuppressWarnings("unchecked")
    private String addCaseGuard(Case c) {
        return pipeline.addGuard(c.name(), (Class<Object>) c.guardType(), (Predicate<Object>) c.guard(), null, null);
    }

    private boolean validateChoose(List<Case> cases) {
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
