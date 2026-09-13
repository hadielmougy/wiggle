package com.wiggle.client.flow;

import com.wiggle.client.dsl.FlowSpec;
import com.wiggle.core.RetryPolicy;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Entry point to the flow API: a workflow written as a chain of method references
 * to its handler methods, compiled to the same {@link FlowSpec} that
 * {@link com.wiggle.client.dsl.Workflow Workflow.define(...)} produces.
 *
 * <pre>{@code
 * OrderHandlers h = new OrderHandlers();
 *
 * FlowSpec order = Wiggle.define("order-fulfilment", Order.class, f -> {
 *     var validated = f.thenApply(h::validate).thenFilter(h::inStock);
 *
 *     var payment  = validated.thenApply(h::charge);
 *     var shipping = validated.thenApply(h::reserve).thenApply(h::label);
 *
 *     return Wiggle.allOf(payment, shipping)
 *                  .combine(h::settle)
 *                  .thenAccept(h::notifyCustomer);
 * });
 *
 * worker.register(order).handlers(h).start();
 * }</pre>
 *
 * <p><b>What this is.</b> A typed front-end over the existing topology DSL, nothing more. The body
 * runs exactly once, here, recording steps; what reaches the server is the same pure topology as
 * before, node for node, and the same content hash. Handlers are still bound by name on the worker;
 * the method references only supply those names in a form the compiler checks and a rename refactor
 * follows. Both APIs can be used in one codebase, and for one workflow they produce identical
 * definitions.
 *
 * <p><b>What it is not.</b> The chain is not executing and the handles are not futures over running
 * work: there is no {@code get()} and no {@code join()}. See {@link WiggleFlow} for what continuing
 * a handle twice means, and for why {@code for} loops in a definition body unroll into nodes.
 *
 * <p>The handler object is used here only as the receiver the method references name; nothing on it
 * is invoked while the workflow is defined. It may be the very instance later given to
 * {@link com.wiggle.client.worker.Worker#handlers Worker.handlers(...)}, which is the point -- one
 * object carries the topology and the logic, still matched by name.
 */
public final class Wiggle {

    private Wiggle() {}

    /**
     * Defines a workflow whose first step takes {@code input}.
     *
     * @param name  the workflow name, as registered with the server
     * @param input the context type the workflow starts from -- it anchors the chain's typing and is
     *              not otherwise used; the graph carries no context type
     * @param body  the chain, run once, here
     */
    public static <T> FlowSpec define(String name, Class<T> input,
                                       Function<WiggleFlow<T>, WiggleFlow<?>> body) {
        return define(name, null, input, body);
    }

    /**
     * {@link #define(String, Class, Function)} with an explicit default retry policy for every step
     * that does not name its own.
     */
    public static <T> FlowSpec define(String name, RetryPolicy defaultRetry, Class<T> input,
                                       Function<WiggleFlow<T>, WiggleFlow<?>> body) {
        if (body == null) throw new IllegalArgumentException("workflow '" + name + "' has no body");
        Plan.Step root = Plan.root();
        WiggleFlow<?> tail = body.apply(new WiggleFlow<>(root));
        if (tail == null) {
            throw new IllegalStateException(
                    "the body of workflow '" + name + "' returned null; it must return the handle it ends on");
        }
        return Plan.compile(name, defaultRetry, root);
    }

    // ------------------------------------------------------------------ fan-out

    /**
     * Fans the flow out over handles that branched from a common point, and returns the mandatory
     * combine stage. The handles are the arms: each runs on its <em>own isolated copy</em> of the
     * context -- an arm's writes are invisible to its siblings and never touch the shared context --
     * so the combine is the only way an arm's result reaches the flow.
     *
     * <pre>{@code
     * var payment  = validated.thenApply(h::charge);
     * var shipping = validated.thenApply(h::label);
     * Wiggle.allOf(payment, shipping).combine(h::settle);
     * }</pre>
     *
     * <p>Unlike {@link java.util.concurrent.CompletableFuture#allOf}, the arms are not already
     * running and the result is not {@code Void}: this records where the graph forks, and the combine
     * that follows records where it rejoins. The arms must fan out from one common step -- that step
     * is where the fork node lands.
     *
     * <p>An arm is named after its last step ({@code charge}, {@code label} above), which is how the
     * engine keys its result for the combine. Step names are already unique within a workflow, so the
     * arm names are too, and nothing has to be declared: a combine that binds
     * {@linkplain com.wiggle.client.worker.Arm by position} never sees them at all, and one that names
     * them with {@code @Arm} uses those.
     */
    public static <A, B> Fork2<A, B> allOf(WiggleFlow<A> a, WiggleFlow<B> b) {
        return new Fork2<>(Plan.fork(steps(a, b)));
    }

    /** {@link #allOf(WiggleFlow, WiggleFlow)} over 3 arms. */
    public static <A, B, C> Fork3<A, B, C> allOf(WiggleFlow<A> a, WiggleFlow<B> b, WiggleFlow<C> c) {
        return new Fork3<>(Plan.fork(steps(a, b, c)));
    }

    /** {@link #allOf(WiggleFlow, WiggleFlow)} over 4 arms. */
    public static <A, B, C, D> Fork4<A, B, C, D> allOf(WiggleFlow<A> a, WiggleFlow<B> b,
                                                           WiggleFlow<C> c, WiggleFlow<D> d) {
        return new Fork4<>(Plan.fork(steps(a, b, c, d)));
    }

    /** {@link #allOf(WiggleFlow, WiggleFlow)} over 5 arms. */
    public static <A, B, C, D, E> Fork5<A, B, C, D, E> allOf(WiggleFlow<A> a, WiggleFlow<B> b,
                                                                 WiggleFlow<C> c, WiggleFlow<D> d,
                                                                 WiggleFlow<E> e) {
        return new Fork5<>(Plan.fork(steps(a, b, c, d, e)));
    }

    /** {@link #allOf(WiggleFlow, WiggleFlow)} over 6 arms. */
    public static <A, B, C, D, E, F> Fork6<A, B, C, D, E, F> allOf(WiggleFlow<A> a, WiggleFlow<B> b,
                                                                       WiggleFlow<C> c, WiggleFlow<D> d,
                                                                       WiggleFlow<E> e, WiggleFlow<F> f) {
        return new Fork6<>(Plan.fork(steps(a, b, c, d, e, f)));
    }

    /** {@link #allOf(WiggleFlow, WiggleFlow)} over 7 arms. */
    public static <A, B, C, D, E, F, G> Fork7<A, B, C, D, E, F, G> allOf(WiggleFlow<A> a,
                                                                             WiggleFlow<B> b,
                                                                             WiggleFlow<C> c,
                                                                             WiggleFlow<D> d,
                                                                             WiggleFlow<E> e,
                                                                             WiggleFlow<F> f,
                                                                             WiggleFlow<G> g) {
        return new Fork7<>(Plan.fork(steps(a, b, c, d, e, f, g)));
    }

    /** {@link #allOf(WiggleFlow, WiggleFlow)} over 8 arms. */
    public static <A, B, C, D, E, F, G, H> Fork8<A, B, C, D, E, F, G, H> allOf(WiggleFlow<A> a,
                                                                                   WiggleFlow<B> b,
                                                                                   WiggleFlow<C> c,
                                                                                   WiggleFlow<D> d,
                                                                                   WiggleFlow<E> e,
                                                                                   WiggleFlow<F> f,
                                                                                   WiggleFlow<G> g,
                                                                                   WiggleFlow<H> h) {
        return new Fork8<>(Plan.fork(steps(a, b, c, d, e, f, g, h)));
    }

    /** {@link #allOf(WiggleFlow, WiggleFlow)} over 9 arms. */
    public static <A, B, C, D, E, F, G, H, I> Fork9<A, B, C, D, E, F, G, H, I> allOf(WiggleFlow<A> a,
                                                                                         WiggleFlow<B> b,
                                                                                         WiggleFlow<C> c,
                                                                                         WiggleFlow<D> d,
                                                                                         WiggleFlow<E> e,
                                                                                         WiggleFlow<F> f,
                                                                                         WiggleFlow<G> g,
                                                                                         WiggleFlow<H> h,
                                                                                         WiggleFlow<I> i) {
        return new Fork9<>(Plan.fork(steps(a, b, c, d, e, f, g, h, i)));
    }

    /** {@link #allOf(WiggleFlow, WiggleFlow)} over 10 arms. */
    public static <A, B, C, D, E, F, G, H, I, J> Fork10<A, B, C, D, E, F, G, H, I, J> allOf(WiggleFlow<A> a,
                                                                                                WiggleFlow<B> b,
                                                                                                WiggleFlow<C> c,
                                                                                                WiggleFlow<D> d,
                                                                                                WiggleFlow<E> e,
                                                                                                WiggleFlow<F> f,
                                                                                                WiggleFlow<G> g,
                                                                                                WiggleFlow<H> h,
                                                                                                WiggleFlow<I> i,
                                                                                                WiggleFlow<J> j) {
        return new Fork10<>(Plan.fork(steps(a, b, c, d, e, f, g, h, i, j)));
    }

    /**
     * {@link #allOf(WiggleFlow, WiggleFlow)} with any number of arms. Past ten the combine's
     * parameters outrun the typed {@code FlowFnN} series, so
     * {@link ForkN#combine(String, Class)} names the combine handler instead of
     * referencing it.
     */
    public static ForkN allOf(WiggleFlow<?>... arms) {
        return new ForkN(Plan.fork(steps(arms)));
    }

    /**
     * {@link #allOf}'s exclusive twin: of the arms given, <em>exactly one</em> runs -- the first whose
     * guard holds, or the {@link WiggleFlow#otherwise} arm if none did. Every arm must open with
     * {@link WiggleFlow#when} or {@code otherwise()}, which is what supplies the guard, and they are
     * evaluated in the order given here.
     *
     * <pre>{@code
     * var vip      = f.when(h::isVip).thenApply(h::vipPath);
     * var standard = f.otherwise().thenApply(h::standardPath);
     * return Wiggle.oneOf(vip, standard);
     * }</pre>
     *
     * <p>Where {@code allOf} needs a combine, this needs nothing: the arms are alternatives on the one
     * context, not parallel branches on isolated copies, so there is no join to merge and control
     * simply continues from whichever arm ran. That is also why the arms must agree on the type they
     * end at -- which one ran is not knowable until run time. Use {@link WiggleFlow#as} after it if
     * they genuinely differ.
     *
     * <p>With no {@code otherwise} arm, a choice where nothing matched skips straight past to the step
     * after it.
     */
    @SafeVarargs
    public static <R> WiggleFlow<R> oneOf(WiggleFlow<R>... arms) {
        return new WiggleFlow<>(Plan.choice(steps(arms)));
    }

    private static List<Plan.Step> steps(WiggleFlow<?>... flows) {
        List<Plan.Step> steps = new ArrayList<>(flows.length);
        for (WiggleFlow<?> flow : flows) {
            if (flow == null) throw new IllegalArgumentException("allOf was given a null branch");
            steps.add(flow.step());
        }
        return steps;
    }
}
