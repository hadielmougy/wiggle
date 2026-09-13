package com.wiggle.client.flow;

import com.wiggle.client.dsl.Branch;
import com.wiggle.client.dsl.Case;
import com.wiggle.client.dsl.ForEachStage;
import com.wiggle.client.dsl.ForkStage;
import com.wiggle.client.dsl.WorkflowBuilder;
import com.wiggle.core.ExecutionMode;
import com.wiggle.core.RetryPolicy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * A handle on the node the flow has reached while it is being <em>defined</em> -- shaped like
 * {@link java.util.concurrent.CompletableFuture} so a workflow reads as a chain, but it is not a
 * future over a running computation. Nothing executes here. Each {@code then*} call appends a node to
 * the topology and returns a handle on the new end; {@link Wiggle#define} runs the chain once and
 * compiles it to the same {@link com.wiggle.client.dsl.Blueprint Blueprint} the name-based DSL
 * produces. The engine, the graph rows and the worker binding are unchanged -- this is a typed
 * front-end, not a second execution model.
 *
 * <p>What the type parameter buys: {@code T} is the context type at this point in the graph, so the
 * compiler checks that the next step's handler actually consumes it, and a rename refactor moves the
 * node name with the method.
 *
 * <pre>{@code
 * Blueprint order = Wiggle.define("order-fulfilment", Order.class, f -> f
 *         .thenApply(account::validate)            // step "validate"   Order -> Order
 *         .thenFilter(account::inStock)            // gate "in-stock"
 *         .thenFork(Arm.of("payment",  a -> a.thenApply(account::charge)),
 *                   Arm.of("shipping", a -> a.thenApply(shipping::reserve)
 *                                             .thenSleep(Duration.ofSeconds(2))
 *                                             .thenApply(shipping::label)))
 *         .combine(account::settle)                // combine "settle"  (Payment, Label) -> Order
 *         .thenAccept(account::notifyCustomer));   // effect "notify-customer"
 * }</pre>
 *
 * <h2>Two rules this API enforces</h2>
 * <ul>
 *   <li><b>No {@code get()} / {@code join()}.</b> They do not exist. A flow describes a graph the
 *       server drives one node at a time; there is nothing to block on while defining it. The only
 *       blocking handle is the client-side one you get back when you <em>start</em> an instance.</li>
 *   <li><b>A future is used once.</b> Continuing the same handle twice would mean two successors of
 *       one node, which is a fan-out -- and fan-out arms run on isolated copies of the context and
 *       must be rejoined by an explicit combine. So say that: {@link #thenFork}. Reusing a handle
 *       throws, naming the fix.</li>
 * </ul>
 *
 * <h2>Java control flow is unrolled</h2>
 * The definition body runs once, so an ordinary {@code for} loop around {@code thenApply} emits that
 * many nodes into the graph -- fine when the bound is a constant, and exactly what you want for
 * generated topologies. Anything that depends on a step's <em>result</em> cannot be an {@code if} or
 * {@code while}: use {@link #thenChoose} and {@link #repeatWhile}, which the engine evaluates at run
 * time.
 *
 * <p>One handler method is one node: node names address the graph and must be unique, so referencing
 * the same method twice in a workflow is a duplicate and is rejected. An unrolled loop therefore
 * means one handler per iteration, not one handler called n times -- for that, use
 * {@link #thenForEach} (parallel, over a collection in the context) or {@link #repeatWhile}
 * (sequential, over a condition the engine evaluates).
 *
 * @param <T> the context type at this point in the graph
 */
public final class WiggleFuture<T> {

    private final WorkflowBuilder stream;
    private boolean continued;

    WiggleFuture(WorkflowBuilder stream) {
        this.stream = stream;
    }

    /** The builder this handle sits on -- for {@link Wiggle} and the fork/forEach stages. */
    WorkflowBuilder stream() {
        return stream;
    }

    /**
     * Claims this handle as continued and hands back the underlying stream. Every operation goes
     * through here, which is what makes a future single-use.
     */
    private WorkflowBuilder take() {
        if (continued) {
            throw new IllegalStateException(
                    "this future was already continued -- a WiggleFuture is single-use, because a node "
                    + "has one successor. To run work in parallel, fan out explicitly: "
                    + "thenFork(Arm.of(\"a\", ...), Arm.of(\"b\", ...)).combine(...) -- each arm runs on "
                    + "its own isolated copy of the context and the combine merges them.");
        }
        continued = true;
        return stream;
    }

    // ------------------------------------------------------------------ steps

    /** A task step: the handler's return value becomes the new context. */
    public <R> WiggleFuture<R> thenApply(FlowFn<T, R> step) {
        return new WiggleFuture<>(take().step(StepNames.of(step)));
    }

    /** {@link #thenApply(FlowFn)} with an explicit retry policy for the step. */
    public <R> WiggleFuture<R> thenApply(FlowFn<T, R> step, RetryPolicy retry) {
        return new WiggleFuture<>(take().step(StepNames.of(step), retry));
    }

    /** {@link #thenApply(FlowFn)} pinned to a dedicated worker queue. */
    public <R> WiggleFuture<R> thenApply(FlowFn<T, R> step, String queue) {
        return new WiggleFuture<>(take().step(StepNames.of(step), queue));
    }

    /** An effect step: the handler returns {@code void}, so the context is unchanged. */
    public WiggleFuture<T> thenAccept(FlowEffect<T> effect) {
        return new WiggleFuture<>(take().effect(StepNames.of(effect)));
    }

    /** {@link #thenAccept(FlowEffect)} with an explicit retry policy. */
    public WiggleFuture<T> thenAccept(FlowEffect<T> effect, RetryPolicy retry) {
        return new WiggleFuture<>(take().effect(StepNames.of(effect), retry));
    }

    /** {@link #thenAccept(FlowEffect)} pinned to a dedicated worker queue. */
    public WiggleFuture<T> thenAccept(FlowEffect<T> effect, String queue) {
        return new WiggleFuture<>(take().effect(StepNames.of(effect), queue));
    }

    /**
     * A guard: the flow continues only while the handler holds. When it does not, the instance ends
     * (or, inside a branch, short-circuits to that branch's join) exactly as {@code gate} does in the
     * name-based DSL.
     */
    public WiggleFuture<T> thenFilter(FlowGate<T> gate) {
        return new WiggleFuture<>(take().gate(StepNames.of(gate)));
    }

    /** {@link #thenFilter(FlowGate)} with an explicit retry policy for the guard. */
    public WiggleFuture<T> thenFilter(FlowGate<T> gate, RetryPolicy retry) {
        return new WiggleFuture<>(take().gate(StepNames.of(gate), retry));
    }

    // ------------------------------------------------------------------ waiting

    /** A server-side timer. No worker is held while the instance waits. */
    public WiggleFuture<T> thenSleep(Duration duration) {
        return new WiggleFuture<>(take().sleep(duration));
    }

    /** {@link #thenSleep(Duration)} under an explicit node name (for a readable console diagram). */
    public WiggleFuture<T> thenSleep(String name, Duration duration) {
        return new WiggleFuture<>(take().sleep(name, duration));
    }

    /**
     * Waits for a signal from an external actor; its payload merges into the context like a step
     * result. The resulting context type is not knowable statically -- re-type it with {@link #as}
     * when the signal changes it.
     */
    public WiggleFuture<T> thenAwait(String signal) {
        return new WiggleFuture<>(take().awaitSignal(signal));
    }

    /** {@link #thenAwait(String)} with a deadline; on timeout the instance fails. */
    public WiggleFuture<T> thenAwait(String signal, Duration timeout) {
        return new WiggleFuture<>(take().awaitSignal(signal, timeout));
    }

    /**
     * {@link #thenAwait(String)} with a deadline and an escalation branch that runs instead when the
     * signal does not arrive in time, rejoining the flow afterwards.
     */
    public WiggleFuture<T> thenAwait(String signal, Duration timeout, UnaryOperator<WiggleFuture<T>> escalation) {
        return new WiggleFuture<>(take().awaitSignal(signal, timeout,
                body(escalation, "the escalation branch of '" + signal + "'")));
    }

    /**
     * Runs another registered workflow as a child instance, starting from this context; its final
     * context merges back here. {@code result} names the type that comes back -- it is not checked
     * against the child's topology, which is a separate definition.
     */
    public <R> WiggleFuture<R> thenSubFlow(String node, String workflow, Class<R> result) {
        return new WiggleFuture<>(take().subWorkflow(node, workflow));
    }

    // ------------------------------------------------------------------ fan-out

    /**
     * Fans out into two parallel arms and returns the mandatory combine stage. Each arm runs on its
     * own isolated copy of the context -- an arm's writes are invisible to its sibling and never
     * touch the shared context -- so {@link Fork2#combine} is the only way its result reaches the
     * flow. A fork left uncombined fails the build.
     */
    public <A, B> Fork2<A, B> thenFork(Arm<T, A> a, Arm<T, B> b) {
        return new Fork2<>(take().fork(branches(List.<Arm<T, ?>>of(a, b))));
    }

    /** Three-armed {@link #thenFork(Arm, Arm)}. */
    public <A, B, C> Fork3<A, B, C> thenFork(Arm<T, A> a, Arm<T, B> b, Arm<T, C> c) {
        return new Fork3<>(take().fork(branches(List.<Arm<T, ?>>of(a, b, c))));
    }

    /**
     * {@link #thenFork(Arm, Arm)} with any number of arms. Past three arms the combine's parameters
     * outrun what a functional interface can express, so {@link ForkN#combine(String, Class)} names
     * the combine handler instead of referencing it.
     */
    @SafeVarargs
    public final ForkN thenFork(Arm<T, ?>... arms) {
        List<Arm<T, ?>> list = new ArrayList<>(arms.length);
        for (Arm<T, ?> arm : arms) list.add(arm);
        return new ForkN(take().fork(branches(list)));
    }

    private Branch[] branches(List<Arm<T, ?>> arms) {
        List<Branch> branches = new ArrayList<>(arms.size());
        for (Arm<T, ?> arm : arms) {
            branches.add(Branch.of(arm.name(), body(arm.body(), "fork arm '" + arm.name() + "'")));
        }
        return branches.toArray(new Branch[0]);
    }

    /**
     * Runtime fan-out: when the instance reaches this node the engine reads the collection stored in
     * the context under {@code itemsKey} and runs the body once per element, in parallel, <b>each
     * element being that branch's whole context</b> (hence {@code itemType}). As with a fork there is
     * no implicit merge -- {@link Items#combine} receives every item's final value and returns the
     * complete post-join context.
     */
    public <E> Items thenForEach(String itemsKey, Class<E> itemType, Function<WiggleFuture<E>, WiggleFuture<?>> body) {
        return new Items(take().forEach(itemsKey, body(body, "the forEach body for '" + itemsKey + "'")));
    }

    /** {@link #thenForEach(String, Class, Function)} under an explicit node name -- needed when the
     *  same collection key is fanned over twice (node names must be unique). */
    public <E> Items thenForEach(String name, String itemsKey, Class<E> itemType,
                                 Function<WiggleFuture<E>, WiggleFuture<?>> body) {
        return new Items(take().forEach(name, itemsKey, body(body, "the forEach body for '" + name + "'")));
    }

    // ------------------------------------------------------------------ branching and looping

    /**
     * Exclusive choice over the context: the first {@link Alt}'s guard to hold runs its branch and the
     * rest are skipped; an {@link Alt#otherwise} arm (which must come last) runs when none matched.
     * Every arm must leave the context type unchanged -- which branch ran is not knowable statically,
     * so there is no honest type to return otherwise. Use {@link #as} after it if a branch does change
     * the shape.
     */
    @SafeVarargs
    public final WiggleFuture<T> thenChoose(Alt<T>... alts) {
        Case[] cases = new Case[alts.length];
        for (int i = 0; i < alts.length; i++) {
            Alt<T> alt = alts[i];
            UnaryOperator<WorkflowBuilder> branch = body(alt.body(), "the branch of case '" + alt.name() + "'");
            cases[i] = alt.guarded() ? Case.when(alt.name(), branch) : Case.otherwise(alt.name(), branch);
        }
        return new WiggleFuture<>(take().choose(cases));
    }

    /**
     * A do-while loop: the body runs once, then the condition is evaluated on a worker, and while it
     * holds the body runs again. Note the ordering -- the body always runs at least once. Compiles to
     * a plain cycle in the graph; the iteration budget is the engine default.
     */
    public WiggleFuture<T> repeatWhile(FlowGate<T> condition, UnaryOperator<WiggleFuture<T>> loopBody) {
        String name = StepNames.of(condition);
        return new WiggleFuture<>(take().doWhile(name, body(loopBody, "the body of loop '" + name + "'")));
    }

    /** {@link #repeatWhile(FlowGate, UnaryOperator)} with an explicit iteration budget: one pass past
     *  {@code maxIterations} fails the instance rather than spinning. */
    public WiggleFuture<T> repeatWhile(FlowGate<T> condition, int maxIterations,
                                       UnaryOperator<WiggleFuture<T>> loopBody) {
        String name = StepNames.of(condition);
        return new WiggleFuture<>(take().doWhile(name, maxIterations,
                body(loopBody, "the body of loop '" + name + "'")));
    }

    // ------------------------------------------------------------------ per-step and workflow settings

    /**
     * Marks the step just added as compensable: if the instance later fails, its undo runs in the
     * reverse pass. Must directly follow {@link #thenApply} or {@link #thenAccept}.
     */
    public WiggleFuture<T> compensate() {
        return new WiggleFuture<>(take().compensate());
    }

    /** Marks the step just added as a flush boundary under {@code LOCAL_ASYNC}. */
    public WiggleFuture<T> checkpoint() {
        return new WiggleFuture<>(take().checkpoint());
    }

    /** Sets the queue used by every step defined after this point. */
    public WiggleFuture<T> defaultQueue(String queue) {
        return new WiggleFuture<>(take().defaultQueue(queue));
    }

    /** Sets how this workflow's steps are driven. Part of the definition's content hash. */
    public WiggleFuture<T> execution(ExecutionMode mode) {
        return new WiggleFuture<>(take().execution(mode));
    }

    /**
     * Re-types the context without touching the graph -- the escape hatch for the places where the
     * new type is not statically knowable ({@link #thenChoose}, {@link #thenAwait}). It adds no node
     * and asserts nothing; the claim is checked where it always was, when the worker decodes the
     * persisted context into the next handler's parameter.
     */
    public <R> WiggleFuture<R> as(Class<R> type) {
        return new WiggleFuture<>(take());
    }

    // ------------------------------------------------------------------ nested bodies

    /**
     * Adapts a typed sub-flow body (arm, case, loop, forEach) to the builder's sub-stream shape. The
     * body is handed a fresh handle on the nested stream and must return the handle it ends on.
     */
    private static <A> UnaryOperator<WorkflowBuilder> body(Function<WiggleFuture<A>, ? extends WiggleFuture<?>> body,
                                                           String what) {
        return sub -> {
            WiggleFuture<?> tail = body.apply(new WiggleFuture<>(sub));
            if (tail == null) {
                throw new IllegalStateException(what + " returned null; it must return the future it ends on");
            }
            return tail.stream;
        };
    }

    /** The stage returned by a two-armed {@link #thenFork(Arm, Arm)}; its combine is mandatory. */
    public static final class Fork2<A, B> {

        private final ForkStage stage;

        Fork2(ForkStage stage) {
            this.stage = stage;
        }

        /**
         * The merge for the preceding fork: a handler whose two {@link com.wiggle.client.worker.Arm
         * @Arm} parameters receive the arms' results in fork order, and whose return is the complete
         * post-join context.
         */
        public <R> WiggleFuture<R> combine(FlowBiFn<A, B, R> combine) {
            return new WiggleFuture<>(stage.combine(StepNames.of(combine)));
        }

        /**
         * The merge named explicitly rather than referenced -- the way to reach a combine handler this
         * API cannot type, in particular one that also takes the pre-fork
         * {@link com.wiggle.client.worker.Context @Context}: the worker matches a combine's parameters
         * by their annotations, not their position, so an extra parameter has no honest place in a
         * two-armed function type.
         */
        public <R> WiggleFuture<R> combine(String name, Class<R> result) {
            return new WiggleFuture<>(stage.combine(name));
        }
    }

    /** The stage returned by a three-armed {@link #thenFork(Arm, Arm, Arm)}; its combine is mandatory. */
    public static final class Fork3<A, B, C> {

        private final ForkStage stage;

        Fork3(ForkStage stage) {
            this.stage = stage;
        }

        /** The merge for the preceding fork; the three parameters are the arms' results in fork order. */
        public <R> WiggleFuture<R> combine(FlowTriFn<A, B, C, R> combine) {
            return new WiggleFuture<>(stage.combine(StepNames.of(combine)));
        }

        /** The merge named explicitly -- see {@link Fork2#combine(String, Class)} for when that is needed. */
        public <R> WiggleFuture<R> combine(String name, Class<R> result) {
            return new WiggleFuture<>(stage.combine(name));
        }
    }

    /** The stage returned by an n-armed {@link #thenFork(Arm[])}; its combine is mandatory. */
    public static final class ForkN {

        private final ForkStage stage;

        ForkN(ForkStage stage) {
            this.stage = stage;
        }

        /**
         * The merge for the preceding fork, named rather than referenced: past three arms the handler's
         * parameter list outruns any functional interface. The handler is the usual one -- an
         * {@link com.wiggle.client.worker.Arm @Arm} parameter per arm.
         */
        public <R> WiggleFuture<R> combine(String name, Class<R> result) {
            return new WiggleFuture<>(stage.combine(name));
        }
    }

    /** The stage returned by {@link #thenForEach}; its combine is mandatory. */
    public static final class Items {

        private final ForEachStage stage;

        Items(ForEachStage stage) {
            this.stage = stage;
        }

        /**
         * The merge for the preceding forEach: a handler taking the collected item results -- a
         * {@code List} ordered by item index when the input was a list, a {@code Map} keyed like the
         * input when it was a map -- and returning the complete post-join context.
         */
        public <X, R> WiggleFuture<R> combine(FlowFn<X, R> combine) {
            return new WiggleFuture<>(stage.combine(StepNames.of(combine)));
        }

        /**
         * {@link #combine(FlowFn)} for a handler that also takes the pre-forEach context: its
         * {@link com.wiggle.client.worker.Context @Context} parameter plus the collected results.
         */
        public <C, X, R> WiggleFuture<R> combine(FlowBiFn<C, X, R> combine) {
            return new WiggleFuture<>(stage.combine(StepNames.of(combine)));
        }

        /** The merge named explicitly. */
        public <R> WiggleFuture<R> combine(String name, Class<R> result) {
            return new WiggleFuture<>(stage.combine(name));
        }
    }
}