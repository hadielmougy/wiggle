package com.wiggle.client.flow;

import com.wiggle.client.dsl.WorkflowBuilder;
import com.wiggle.core.ExecutionMode;
import com.wiggle.core.RetryPolicy;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * A handle on the point the flow has reached while it is being <em>defined</em> -- shaped like
 * {@link java.util.concurrent.CompletableFuture} so a workflow reads as a chain, but it is not a
 * future over a running computation. Nothing executes here. Each {@code then*} call records a step
 * and returns a handle on the new end; {@link Wiggle#define} walks the recording once and compiles it
 * to the same {@link com.wiggle.client.dsl.FlowSpec FlowSpec} the name-based DSL produces. The
 * engine, the graph rows and the worker binding are unchanged -- this is a typed front-end, not a
 * second execution model.
 *
 * <p>What the type parameter buys: {@code T} is the context type at this point in the graph, so the
 * compiler checks that the next step's handler actually consumes it, and a rename refactor moves the
 * node name with the method.
 *
 * <pre>{@code
 * FlowSpec order = Wiggle.define("order-fulfilment", Order.class, f -> {
 *     var validated = f.thenApply(h::validate).thenFilter(h::inStock);
 *
 *     var payment  = validated.thenApply(h::charge);
 *     var shipping = validated.thenApply(h::reserve)
 *                             .thenSleep(Duration.ofSeconds(2))
 *                             .thenApply(h::label);
 *
 *     return Wiggle.allOf(payment, shipping)
 *                  .combine(h::settle)
 *                  .thenAccept(h::notifyCustomer);
 * });
 * }</pre>
 *
 * <h2>Continuing a handle twice is a fan-out</h2>
 * {@code validated} above is continued twice, and that is exactly what makes the two arms. They must
 * be rejoined -- branches run on isolated copies of the context, so a combine is the only way their
 * results reach the flow. A split that is never passed to {@link Wiggle#allOf} is rejected when the
 * workflow is defined, naming the two ends that were left dangling.
 *
 * <p>There is no {@code get()} or {@code join()}, and there never will be: a flow describes a graph
 * the server drives one node at a time, so there is nothing to block on while describing it. The only
 * blocking handle is the client-side one returned when an instance is <em>started</em>.
 *
 * <h2>Java control flow is unrolled</h2>
 * The definition body runs once, so an ordinary {@code for} loop around {@code thenApply} records that
 * many steps -- fine when the bound is a constant, and what you want for generated topologies.
 * Anything that depends on a step's <em>result</em> cannot be an {@code if} or {@code while}: use
 * {@link Wiggle#oneOf} and {@link #repeatWhile}, which the engine evaluates at run time.
 *
 * <p>One handler method is one node: node names address the graph and must be unique, so referencing
 * the same method twice in a workflow is a duplicate and is rejected. An unrolled loop therefore
 * means one handler per iteration, not one handler called n times -- for that, use
 * {@link #thenForEach} (parallel, over a collection in the context) or {@link #repeatWhile}
 * (sequential, over a condition the engine evaluates).
 *
 * @param <T> the context type at this point in the graph
 */
public final class WiggleFlow<T> {

    private final Plan.Step step;

    WiggleFlow(Plan.Step step) {
        this.step = step;
    }

    /** This handle's position in the recording -- for {@link Wiggle} and the fork stages. */
    Plan.Step step() {
        return step;
    }

    /** Records an operation and returns a handle on it. */
    private <R> WiggleFlow<R> record(String label, UnaryOperator<WorkflowBuilder> op) {
        return new WiggleFlow<>(new Plan.Step(step, op, label));
    }

    // ------------------------------------------------------------------ steps

    /**
     * A task step: the handler's return value becomes the new context.
     *
     * <p>Every step kind below takes an optional {@link RetryPolicy} and an optional queue, in either
     * order, so any combination reads the way you want to write it. A step with no policy inherits the
     * workflow default given to {@link Wiggle#define(String, RetryPolicy, Class, Function)}; a step
     * with no queue uses the workflow's {@link #defaultQueue}.
     */
    public <R> WiggleFlow<R> thenApply(FlowFn<T, R> step) {
        return task(step, null, null);
    }

    /** {@link #thenApply(FlowFn)} with an explicit retry policy for the step. */
    public <R> WiggleFlow<R> thenApply(FlowFn<T, R> step, RetryPolicy retry) {
        return task(step, retry, null);
    }

    /** {@link #thenApply(FlowFn)} pinned to a dedicated worker queue. */
    public <R> WiggleFlow<R> thenApply(FlowFn<T, R> step, String queue) {
        return task(step, null, queue);
    }

    /** {@link #thenApply(FlowFn)} with both a retry policy and a dedicated queue. */
    public <R> WiggleFlow<R> thenApply(FlowFn<T, R> step, RetryPolicy retry, String queue) {
        return task(step, retry, queue);
    }

    /** {@link #thenApply(FlowFn, RetryPolicy, String)}, queue first. */
    public <R> WiggleFlow<R> thenApply(FlowFn<T, R> step, String queue, RetryPolicy retry) {
        return task(step, retry, queue);
    }

    private <R> WiggleFlow<R> task(FlowFn<T, R> step, RetryPolicy retry, String queue) {
        String name = StepNames.of(step);
        return record(name, b -> b.step(name, retry, queue));
    }

    /** An effect step: the handler returns {@code void}, so the context is unchanged. */
    public WiggleFlow<T> thenAccept(FlowEffect<T> effect) {
        return effect(effect, null, null);
    }

    /** {@link #thenAccept(FlowEffect)} with an explicit retry policy. */
    public WiggleFlow<T> thenAccept(FlowEffect<T> effect, RetryPolicy retry) {
        return effect(effect, retry, null);
    }

    /** {@link #thenAccept(FlowEffect)} pinned to a dedicated worker queue. */
    public WiggleFlow<T> thenAccept(FlowEffect<T> effect, String queue) {
        return effect(effect, null, queue);
    }

    /** {@link #thenAccept(FlowEffect)} with both a retry policy and a dedicated queue. */
    public WiggleFlow<T> thenAccept(FlowEffect<T> effect, RetryPolicy retry, String queue) {
        return effect(effect, retry, queue);
    }

    /** {@link #thenAccept(FlowEffect, RetryPolicy, String)}, queue first. */
    public WiggleFlow<T> thenAccept(FlowEffect<T> effect, String queue, RetryPolicy retry) {
        return effect(effect, retry, queue);
    }

    private WiggleFlow<T> effect(FlowEffect<T> effect, RetryPolicy retry, String queue) {
        String name = StepNames.of(effect);
        return record(name, b -> b.effect(name, retry, queue));
    }

    /**
     * A guard: the flow continues only while the handler holds. When it does not, the instance ends
     * (or, inside a branch, short-circuits to that branch's join) exactly as {@code gate} does in the
     * name-based DSL.
     */
    public WiggleFlow<T> thenFilter(FlowGate<T> gate) {
        return gate(gate, null, null);
    }

    /** {@link #thenFilter(FlowGate)} with an explicit retry policy for the guard. */
    public WiggleFlow<T> thenFilter(FlowGate<T> gate, RetryPolicy retry) {
        return gate(gate, retry, null);
    }

    /** {@link #thenFilter(FlowGate)} pinned to a dedicated worker queue. */
    public WiggleFlow<T> thenFilter(FlowGate<T> gate, String queue) {
        return gate(gate, null, queue);
    }

    /** {@link #thenFilter(FlowGate)} with both a retry policy and a dedicated queue. */
    public WiggleFlow<T> thenFilter(FlowGate<T> gate, RetryPolicy retry, String queue) {
        return gate(gate, retry, queue);
    }

    /** {@link #thenFilter(FlowGate, RetryPolicy, String)}, queue first. */
    public WiggleFlow<T> thenFilter(FlowGate<T> gate, String queue, RetryPolicy retry) {
        return gate(gate, retry, queue);
    }

    private WiggleFlow<T> gate(FlowGate<T> gate, RetryPolicy retry, String queue) {
        String name = StepNames.of(gate);
        return record(name, b -> b.gate(name, retry, queue));
    }

    // ------------------------------------------------------------------ waiting

    /** A server-side timer. No worker is held while the instance waits. */
    public WiggleFlow<T> thenSleep(Duration duration) {
        // same generated name the builder gives it, so an arm ending here still has one
        return record("sleep-" + duration.toMillis() + "ms", b -> b.sleep(duration));
    }

    /** {@link #thenSleep(Duration)} under an explicit node name (for a readable console diagram). */
    public WiggleFlow<T> thenSleep(String name, Duration duration) {
        return record(name, b -> b.sleep(name, duration));
    }

    /**
     * Waits for a signal from an external actor; its payload merges into the context like a step
     * result. The resulting context type is not knowable statically -- re-type it with {@link #as}
     * when the signal changes it.
     */
    public WiggleFlow<T> thenAwait(String signal) {
        return record(signal, b -> b.awaitSignal(signal));
    }

    /** {@link #thenAwait(String)} with a deadline; on timeout the instance fails. */
    public WiggleFlow<T> thenAwait(String signal, Duration timeout) {
        return record(signal, b -> b.awaitSignal(signal, timeout));
    }

    /**
     * {@link #thenAwait(String)} with a deadline and an escalation branch that runs instead when the
     * signal does not arrive in time, rejoining the flow afterwards.
     */
    public WiggleFlow<T> thenAwait(String signal, Duration timeout, UnaryOperator<WiggleFlow<T>> escalation) {
        UnaryOperator<WorkflowBuilder> body = body(escalation, "the escalation branch of '" + signal + "'");
        return record(signal, b -> b.awaitSignal(signal, timeout, body));
    }

    /**
     * Runs another registered workflow as a child instance, starting from this context; its final
     * context merges back here. {@code result} names the type that comes back -- it is not checked
     * against the child's topology, which is a separate definition.
     */
    public <R> WiggleFlow<R> thenSubFlow(String node, String workflow, Class<R> result) {
        return record(node, b -> b.subWorkflow(node, workflow));
    }

    // ------------------------------------------------------------------ dynamic fan-out

    /**
     * Runtime fan-out: when the instance reaches this node the engine reads the collection stored in
     * the context under {@code itemsKey} and runs the body once per element, in parallel, <b>each
     * element being that branch's whole context</b> (hence {@code itemType}). As with
     * {@link Wiggle#allOf} there is no implicit merge -- {@link Items#combine} receives every item's
     * final value and returns the complete post-join context.
     */
    public <E> Items thenForEach(String itemsKey, Class<E> itemType,
                                 Function<WiggleFlow<E>, WiggleFlow<?>> loopBody) {
        return thenForEach(itemsKey, itemsKey, itemType, loopBody);
    }

    /** {@link #thenForEach(String, Class, Function)} under an explicit node name -- needed when the
     *  same collection key is fanned over twice (node names must be unique). */
    public <E> Items thenForEach(String name, String itemsKey, Class<E> itemType,
                                 Function<WiggleFlow<E>, WiggleFlow<?>> loopBody) {
        UnaryOperator<WorkflowBuilder> body = body(loopBody, "the forEach body for '" + name + "'");
        return new Items(this, name, itemsKey, body);
    }

    // ------------------------------------------------------------------ branching and looping

    /**
     * Opens an arm of a {@link Wiggle#oneOf}: the steps chained after this run only when {@code guard}
     * is the first of the choice's guards to hold.
     *
     * <pre>{@code
     * var vip      = f.when(h::isVip).thenApply(h::vipPath);
     * var standard = f.otherwise().thenApply(h::standardPath);
     * return Wiggle.oneOf(vip, standard);
     * }</pre>
     *
     * <p>This is not {@link #thenFilter}: a gate that fails ends the instance (or short-circuits its
     * branch), while a guard that fails hands the choice to the next arm. The marker records no node
     * of its own -- {@code oneOf} builds the guards, in the order the arms are given to it.
     */
    public WiggleFlow<T> when(FlowGate<T> guard) {
        return guarded(guard, null, null);
    }

    /** {@link #when(FlowGate)} with an explicit retry policy for the guard. */
    public WiggleFlow<T> when(FlowGate<T> guard, RetryPolicy retry) {
        return guarded(guard, retry, null);
    }

    /** {@link #when(FlowGate)} with the guard pinned to a dedicated worker queue. */
    public WiggleFlow<T> when(FlowGate<T> guard, String queue) {
        return guarded(guard, null, queue);
    }

    /** {@link #when(FlowGate)} with both a retry policy and a dedicated queue. */
    public WiggleFlow<T> when(FlowGate<T> guard, RetryPolicy retry, String queue) {
        return guarded(guard, retry, queue);
    }

    /** {@link #when(FlowGate, RetryPolicy, String)}, queue first. */
    public WiggleFlow<T> when(FlowGate<T> guard, String queue, RetryPolicy retry) {
        return guarded(guard, retry, queue);
    }

    private WiggleFlow<T> guarded(FlowGate<T> guard, RetryPolicy retry, String queue) {
        return new WiggleFlow<>(new Plan.Guard(step, StepNames.of(guard), retry, queue, false));
    }

    /**
     * Opens the default arm of a {@link Wiggle#oneOf}: it runs when none of the choice's guards held.
     * It has no guard of its own, so it takes no retry or queue, and it must be the last arm given to
     * {@code oneOf}.
     */
    public WiggleFlow<T> otherwise() {
        return new WiggleFlow<>(new Plan.Guard(step, null, null, null, true));
    }

    /**
     * A do-while loop: the body runs once, then the condition is evaluated on a worker, and while it
     * holds the body runs again. Note the ordering -- the body always runs at least once. Compiles to
     * a plain cycle in the graph; the iteration budget is the engine default.
     */
    public WiggleFlow<T> repeatWhile(FlowGate<T> condition, UnaryOperator<WiggleFlow<T>> loopBody) {
        String name = StepNames.of(condition);
        UnaryOperator<WorkflowBuilder> body = body(loopBody, "the body of loop '" + name + "'");
        return record(name, b -> b.doWhile(name, body));
    }

    /** {@link #repeatWhile(FlowGate, UnaryOperator)} with an explicit iteration budget: one pass past
     *  {@code maxIterations} fails the instance rather than spinning. */
    public WiggleFlow<T> repeatWhile(FlowGate<T> condition, int maxIterations,
                                       UnaryOperator<WiggleFlow<T>> loopBody) {
        String name = StepNames.of(condition);
        UnaryOperator<WorkflowBuilder> body = body(loopBody, "the body of loop '" + name + "'");
        return record(name, b -> b.doWhile(name, maxIterations, body));
    }

    // ------------------------------------------------------------------ per-step and workflow settings

    /**
     * Gives the step just recorded an explicit retry policy, overriding the workflow default. The
     * inline forms above cover the common case; this reaches what they cannot -- a combine, and a
     * {@link #repeatWhile} condition:
     *
     * <pre>{@code
     * Wiggle.allOf(payment, shipping).combine(h::settle).withRetry(patient)
     * f.repeatWhile(h::hasMore, a -> a.thenApply(h::drain)).withRetry(gentle)
     * }</pre>
     *
     * Applies to whatever a worker runs -- step, effect, gate, combine or loop condition -- and fails
     * on anything the engine runs itself (a sleep, a signal wait, a sub-workflow), which has no
     * worker to retry on.
     */
    public WiggleFlow<T> withRetry(RetryPolicy retry) {
        return record(null, b -> b.withRetry(retry));
    }

    /**
     * Pins the step just recorded to a dedicated worker queue, overriding the workflow default. Like
     * {@link #withRetry}, this reaches the nodes with no inline form and applies to whatever a worker
     * runs.
     */
    public WiggleFlow<T> onQueue(String queue) {
        return record(null, b -> b.onQueue(queue));
    }

    /**
     * Marks the step just added as compensable: if the instance later fails, its undo runs in the
     * reverse pass. Must directly follow {@link #thenApply} or {@link #thenAccept}.
     */
    public WiggleFlow<T> compensate() {
        return record(null, WorkflowBuilder::compensate);
    }

    /** Marks the step just added as a flush boundary under {@code LOCAL_ASYNC}. */
    public WiggleFlow<T> checkpoint() {
        return record(null, WorkflowBuilder::checkpoint);
    }

    /** Sets the queue used by every step defined after this point. */
    public WiggleFlow<T> defaultQueue(String queue) {
        return record(null, b -> b.defaultQueue(queue));
    }

    /** Sets how this workflow's steps are driven. Part of the definition's content hash. */
    public WiggleFlow<T> execution(ExecutionMode mode) {
        return record(null, b -> b.execution(mode));
    }

    /**
     * Re-types the context without recording anything -- the escape hatch for the places where the
     * new type is not statically knowable ({@link Wiggle#oneOf}, {@link #thenAwait}). It adds no node
     * and asserts nothing; the claim is checked where it always was, when the worker decodes the
     * persisted context into the next handler's parameter.
     */
    @SuppressWarnings("unchecked")
    public <R> WiggleFlow<R> as(Class<R> type) {
        return (WiggleFlow<R>) this;
    }

    // ------------------------------------------------------------------ nested bodies

    /**
     * Compiles a nested body (escalation, case, loop, forEach) into the builder's sub-stream shape.
     * The body records its own little tree, which is walked into the nested builder when the enclosing
     * step is applied.
     */
    private static <A> UnaryOperator<WorkflowBuilder> body(Function<WiggleFlow<A>, ? extends WiggleFlow<?>> body,
                                                           String what) {
        Plan.Step root = Plan.root();
        WiggleFlow<?> tail = body.apply(new WiggleFlow<>(root));
        if (tail == null) {
            throw new IllegalStateException(what + " returned null; it must return the handle it ends on");
        }
        return sub -> Plan.walk(root, sub, what);
    }

    // ------------------------------------------------------------------ fork stages

    /**
     * The stage returned by a two-armed {@link Wiggle#allOf}; its combine is mandatory.
     *
     * <p>A referenced combine is checked against the fork while the workflow is being defined: the
     * handler's {@link com.wiggle.client.worker.Arm @Arm} parameters must name this fork's arms, in
     * the order the handles were given. That is what makes {@code A} and {@code B} above mean
     * anything -- and it turns a mistyped arm name, which the engine can only discover when the
     * combine runs, into an error at definition time.
     */
    public static final class Fork2<A, B> {

        private final Plan.Fork fork;

        Fork2(Plan.Fork fork) {
            this.fork = fork;
        }

        /**
         * The merge for the preceding fan-out: a handler whose two {@code @Arm} parameters receive the
         * arms' results in the order given to {@code allOf}, and whose return is the complete
         * post-join context.
         */
        public <R> WiggleFlow<R> combine(FlowBiFn<A, B, R> combine) {
            return merge(StepNames.ofCombine(combine, fork.armNames, false));
        }

        /**
         * {@link #combine(FlowBiFn)} for a merge that also needs the pre-fork context: a handler
         * declared as {@code (@Context C base, @Arm(..) A a, @Arm(..) B b)}. The context parameter
         * comes first so the type arguments line up with the declaration.
         */
        public <C, R> WiggleFlow<R> combineWithContext(FlowTriFn<C, A, B, R> combine) {
            return merge(StepNames.ofCombine(combine, fork.armNames, true));
        }

        /**
         * The merge named explicitly rather than referenced -- the escape hatch for a combine handler
         * this API cannot type, and the only form that skips the arm check above.
         */
        public <R> WiggleFlow<R> combine(String name, Class<R> result) {
            return merge(name);
        }

        private <R> WiggleFlow<R> merge(String name) {
            fork.combineName = name;
            return new WiggleFlow<>(fork);
        }
    }

    /** The stage returned by a three-armed {@link Wiggle#allOf}; its combine is mandatory. */
    public static final class Fork3<A, B, C> {

        private final Plan.Fork fork;

        Fork3(Plan.Fork fork) {
            this.fork = fork;
        }

        /** The merge for the preceding fan-out; the three parameters are the arms' results in order. */
        public <R> WiggleFlow<R> combine(FlowTriFn<A, B, C, R> combine) {
            fork.combineName = StepNames.ofCombine(combine, fork.armNames, false);
            return new WiggleFlow<>(fork);
        }

        /** The merge named explicitly -- see {@link Fork2#combine(String, Class)} for when that is needed.
         *  A three-armed combine that also takes the {@code @Context} uses this form. */
        public <R> WiggleFlow<R> combine(String name, Class<R> result) {
            fork.combineName = name;
            return new WiggleFlow<>(fork);
        }
    }

    /** The stage returned by an n-armed {@link Wiggle#allOf}; its combine is mandatory. */
    public static final class ForkN {

        private final Plan.Fork fork;

        ForkN(Plan.Fork fork) {
            this.fork = fork;
        }

        /**
         * The merge for the preceding fan-out, named rather than referenced: past three arms the
         * handler's parameter list outruns any functional interface. The handler is the usual one --
         * an {@link com.wiggle.client.worker.Arm @Arm} parameter per arm.
         */
        public <R> WiggleFlow<R> combine(String name, Class<R> result) {
            fork.combineName = name;
            return new WiggleFlow<>(fork);
        }
    }

    /** The stage returned by {@link #thenForEach}; its combine is mandatory. */
    public static final class Items {

        private final WiggleFlow<?> from;
        private final String name;
        private final String itemsKey;
        private final UnaryOperator<WorkflowBuilder> body;

        Items(WiggleFlow<?> from, String name, String itemsKey, UnaryOperator<WorkflowBuilder> body) {
            this.from = from;
            this.name = name;
            this.itemsKey = itemsKey;
            this.body = body;
        }

        /**
         * The merge for the preceding forEach: a handler taking the collected item results -- a
         * {@code List} ordered by item index when the input was a list, a {@code Map} keyed like the
         * input when it was a map -- and returning the complete post-join context.
         */
        public <X, R> WiggleFlow<R> combine(FlowFn<X, R> combine) {
            return merge(StepNames.of(combine));
        }

        /**
         * {@link #combine(FlowFn)} for a handler that also takes the pre-forEach context: its
         * {@link com.wiggle.client.worker.Context @Context} parameter plus the collected results.
         */
        public <C, X, R> WiggleFlow<R> combine(FlowBiFn<C, X, R> combine) {
            return merge(StepNames.of(combine));
        }

        /** The merge named explicitly. */
        public <R> WiggleFlow<R> combine(String name, Class<R> result) {
            return merge(name);
        }

        private <R> WiggleFlow<R> merge(String combineName) {
            return from.record(name, b -> b.forEach(name, itemsKey, body).combine(combineName));
        }
    }
}
