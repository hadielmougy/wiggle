package com.wiggle.client.worker;

import com.wiggle.client.flow.FlowEffect;
import com.wiggle.client.flow.FlowFactory;
import com.wiggle.client.flow.FlowFn;
import com.wiggle.client.flow.FlowFn2;
import com.wiggle.client.flow.FlowFn3;
import com.wiggle.client.flow.FlowFn4;
import com.wiggle.client.flow.FlowGate;
import com.wiggle.client.flow.StepNames;
import com.wiggle.core.CreatedBranch;
import com.wiggle.core.CreatedBranch.BranchStep;
import com.wiggle.core.NodeKind;
import com.wiggle.core.RecordMapper;
import com.wiggle.core.RetryPolicy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * A branch the running step creates, opened by {@link Step#create}. It chains steps with the
 * operators that author a workflow, named by method reference -- {@code this::reserve} on the
 * handler object, or {@code Steps::reserve} on the contract -- and nothing referenced is invoked
 * here. The branch runs on its own isolated context, its input, once the step's report is committed;
 * its result reaches the combine after the step.
 *
 * <p>A step without its own queue or retry policy takes the creating step's. A step on a queue no
 * graph mentions runs only on a worker that lists that queue in {@code WorkerOptions.queues()}.
 *
 * @param <I> the branch's context type at this point
 */
public final class Branch<I> {

    private final Object input;
    private final String key;
    private final List<BranchStep> steps = new ArrayList<>(4);
    /** Whether this is an arm of {@link #thenOneOf}, and the guard that opened it, or {@code otherwise}. */
    private boolean choiceArm;
    private String guard;
    private boolean otherwise;

    private Branch(Object input, String key) {
        this.input = input;
        this.key = key;
    }

    /** Opens a branch over {@code input}, taken as JSON now so later changes to it do not leak in. */
    static <I> Branch<I> open(I input, String key) {
        return new Branch<>(RecordMapper.toJson(input), key);
    }

    CreatedBranch build() {
        return new CreatedBranch(input, key, steps);
    }

    public <R> Branch<R> thenApply(FlowFn<? super I, R> step) {
        return add(StepNames.ofBranchStep(step), NodeKind.TASK, false, null, null);
    }

    public <R> Branch<R> thenApply(FlowFn<? super I, R> step, RetryPolicy retry) {
        return add(StepNames.ofBranchStep(step), NodeKind.TASK, false, retry, null);
    }

    public <R> Branch<R> thenApply(FlowFn<? super I, R> step, String queue) {
        return add(StepNames.ofBranchStep(step), NodeKind.TASK, false, null, queue);
    }

    public <R> Branch<R> thenApply(FlowFn<? super I, R> step, RetryPolicy retry, String queue) {
        return add(StepNames.ofBranchStep(step), NodeKind.TASK, false, retry, queue);
    }

    public <R> Branch<R> thenApply(FlowFn<? super I, R> step, String queue, RetryPolicy retry) {
        return add(StepNames.ofBranchStep(step), NodeKind.TASK, false, retry, queue);
    }

    public Branch<I> thenAccept(FlowEffect<? super I> effect) {
        return add(StepNames.ofBranchStep(effect), NodeKind.TASK, false, null, null);
    }

    public Branch<I> thenAccept(FlowEffect<? super I> effect, RetryPolicy retry) {
        return add(StepNames.ofBranchStep(effect), NodeKind.TASK, false, retry, null);
    }

    public Branch<I> thenAccept(FlowEffect<? super I> effect, String queue) {
        return add(StepNames.ofBranchStep(effect), NodeKind.TASK, false, null, queue);
    }

    public Branch<I> thenAccept(FlowEffect<? super I> effect, RetryPolicy retry, String queue) {
        return add(StepNames.ofBranchStep(effect), NodeKind.TASK, false, retry, queue);
    }

    public Branch<I> thenAccept(FlowEffect<? super I> effect, String queue, RetryPolicy retry) {
        return add(StepNames.ofBranchStep(effect), NodeKind.TASK, false, retry, queue);
    }

    /** A gate: false ends this branch at the join, as a false gate ends a fork arm. */
    public Branch<I> thenFilter(FlowGate<? super I> gate) {
        return add(StepNames.ofBranchStep(gate), NodeKind.PREDICATE, false, null, null);
    }

    public Branch<I> thenFilter(FlowGate<? super I> gate, RetryPolicy retry) {
        return add(StepNames.ofBranchStep(gate), NodeKind.PREDICATE, false, retry, null);
    }

    public Branch<I> thenFilter(FlowGate<? super I> gate, String queue) {
        return add(StepNames.ofBranchStep(gate), NodeKind.PREDICATE, false, null, queue);
    }

    public Branch<I> thenFilter(FlowGate<? super I> gate, RetryPolicy retry, String queue) {
        return add(StepNames.ofBranchStep(gate), NodeKind.PREDICATE, false, retry, queue);
    }

    public Branch<I> thenFilter(FlowGate<? super I> gate, String queue, RetryPolicy retry) {
        return add(StepNames.ofBranchStep(gate), NodeKind.PREDICATE, false, retry, queue);
    }

    /** A step whose undo runs in the reverse pass if the instance later fails. */
    public <R> Branch<R> thenApplyCompensable(FlowFactory<? extends CompensableActivity<?, R>> factory) {
        return add(StepNames.ofBranchStep(factory), NodeKind.TASK, true, null, null);
    }

    public <R> Branch<R> thenApplyCompensable(FlowFactory<? extends CompensableActivity<?, R>> factory,
                                              RetryPolicy retry) {
        return add(StepNames.ofBranchStep(factory), NodeKind.TASK, true, retry, null);
    }

    public <R> Branch<R> thenApplyCompensable(FlowFactory<? extends CompensableActivity<?, R>> factory,
                                              String queue) {
        return add(StepNames.ofBranchStep(factory), NodeKind.TASK, true, null, queue);
    }

    public <R> Branch<R> thenApplyCompensable(FlowFactory<? extends CompensableActivity<?, R>> factory,
                                              RetryPolicy retry, String queue) {
        return add(StepNames.ofBranchStep(factory), NodeKind.TASK, true, retry, queue);
    }

    public <R> Branch<R> thenApplyCompensable(FlowFactory<? extends CompensableActivity<?, R>> factory,
                                              String queue, RetryPolicy retry) {
        return add(StepNames.ofBranchStep(factory), NodeKind.TASK, true, retry, queue);
    }

    /**
     * Makes the step just chained create branches of its own: inside its handler,
     * {@code Step.create} opens them, and {@code combine} receives their results, as the combine
     * after a spawning step does in a workflow. The branch goes on with the combine's return. The
     * combine runs on the step's queue, under its retry policy.
     */
    public <P1, R> Branch<R> combine(FlowFn<P1, R> combine) {
        return nest(StepNames.ofBranchStep(combine));
    }

    public <P1, P2, R> Branch<R> combine(FlowFn2<P1, P2, R> combine) {
        return nest(StepNames.ofBranchStep(combine));
    }

    public <P1, P2, P3, R> Branch<R> combine(FlowFn3<P1, P2, P3, R> combine) {
        return nest(StepNames.ofBranchStep(combine));
    }

    public <P1, P2, P3, P4, R> Branch<R> combine(FlowFn4<P1, P2, P3, P4, R> combine) {
        return nest(StepNames.ofBranchStep(combine));
    }

    @SuppressWarnings("unchecked")
    private <R> Branch<R> nest(String combineName) {
        BranchStep last = steps.isEmpty() ? null : steps.getLast();
        if (last == null || last.kind() != NodeKind.TASK || last.combine() != null) {
            throw new IllegalStateException("combine(" + combineName + ") must directly follow the step "
                    + "that creates the branches it combines: a thenApply or thenAccept");
        }
        steps.set(steps.size() - 1, last.withCombine(combineName));
        return (Branch<R>) this;
    }

    /** Waits, without holding a worker, for the signal {@code signal} delivered to the instance. */
    public Branch<I> thenAwait(String signal) {
        steps.add(CreatedBranch.BranchStep.await(signal, 0, List.of()));
        return this;
    }

    /** {@link #thenAwait(String)} with a deadline; when it passes, the instance fails. */
    public Branch<I> thenAwait(String signal, Duration timeout) {
        return thenAwait(signal, timeout, null);
    }

    /**
     * {@link #thenAwait(String)} with a deadline; when it passes, {@code escalation} runs instead and
     * the branch goes on after it, as it does after the signal.
     */
    public Branch<I> thenAwait(String signal, Duration timeout, UnaryOperator<Branch<I>> escalation) {
        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("a wait's timeout must be positive: " + timeout);
        }
        List<BranchStep> onTimeout = escalation == null ? List.of() : chain(escalation, "the escalation of '" + signal + "'");
        steps.add(CreatedBranch.BranchStep.await(signal, timeout.toMillis(), onTimeout));
        return this;
    }

    /**
     * Runs the workflow {@code workflow} as a child; its final context merges into this branch's
     * when it completes, and the branch fails if it does not. {@code node} names the step.
     */
    @SuppressWarnings("unchecked")
    public <R> Branch<R> thenSubFlow(String node, String workflow, Class<R> result) {
        steps.add(CreatedBranch.BranchStep.subFlow(node, workflow));
        return (Branch<R>) this;
    }

    /**
     * Runs {@code arms} in parallel, each on its own copy of this branch's context; the
     * {@link Arms#combine combine} merges them, as after {@code Wiggle.allOf}. An arm is named after
     * its last step, which must be a task or a gate.
     */
    @SafeVarargs
    public final Arms thenAllOf(Function<Branch<I>, Branch<?>>... arms) {
        if (arms.length < 2) throw new IllegalArgumentException("thenAllOf needs at least two arms");
        List<List<BranchStep>> chains = new ArrayList<>(arms.length);
        for (int a = 0; a < arms.length; a++) {
            Function<Branch<I>, Branch<?>> arm = arms[a];
            chains.add(chain(b -> { arm.apply(b); return b; }, "arm " + a));
        }
        return new Arms(chains);
    }

    /** The mandatory merge after {@link #thenAllOf}: its parameters are found by type, in any order. */
    public final class Arms {

        private final List<List<BranchStep>> arms;

        private Arms(List<List<BranchStep>> arms) {
            this.arms = arms;
        }

        public <P1, R> Branch<R> combine(FlowFn<P1, R> combine) {
            return fork(StepNames.ofBranchStep(combine));
        }

        public <P1, P2, R> Branch<R> combine(FlowFn2<P1, P2, R> combine) {
            return fork(StepNames.ofBranchStep(combine));
        }

        public <P1, P2, P3, R> Branch<R> combine(FlowFn3<P1, P2, P3, R> combine) {
            return fork(StepNames.ofBranchStep(combine));
        }

        public <P1, P2, P3, P4, R> Branch<R> combine(FlowFn4<P1, P2, P3, P4, R> combine) {
            return fork(StepNames.ofBranchStep(combine));
        }

        @SuppressWarnings("unchecked")
        private <R> Branch<R> fork(String combineName) {
            steps.add(CreatedBranch.BranchStep.fork(arms, combineName));
            return (Branch<R>) Branch.this;
        }
    }

    /**
     * Runs {@code body}, then again while {@code condition} holds -- a do-while, as in a workflow --
     * failing the instance past the engine's iteration budget.
     */
    public Branch<I> repeatWhile(FlowGate<? super I> condition, UnaryOperator<Branch<I>> body) {
        return loop(condition, 0, body);
    }

    /** {@link #repeatWhile(FlowGate, UnaryOperator)} failing past {@code maxIterations}. */
    public Branch<I> repeatWhile(FlowGate<? super I> condition, int maxIterations, UnaryOperator<Branch<I>> body) {
        if (maxIterations <= 0) throw new IllegalArgumentException("maxIterations must be positive: " + maxIterations);
        return loop(condition, maxIterations, body);
    }

    private Branch<I> loop(FlowGate<? super I> condition, int maxIterations, UnaryOperator<Branch<I>> body) {
        String name = StepNames.ofBranchStep(condition);
        steps.add(BranchStep.loop(name, chain(body, "the body of repeatWhile(" + name + ")"), maxIterations));
        return this;
    }

    /**
     * Runs the first of {@code arms} whose guard holds, as {@code Wiggle.oneOf} does: each arm opens
     * with {@link #when} or, last, {@link #otherwise}. Without an {@code otherwise}, a choice where no
     * guard held goes on past it. The arms are alternatives on this branch's context, so there is no
     * combine.
     */
    @SafeVarargs
    @SuppressWarnings("unchecked")
    public final <R> Branch<R> thenOneOf(Function<Branch<I>, Branch<R>>... arms) {
        List<BranchStep.Case> cases = new ArrayList<>(arms.length);
        for (int a = 0; a < arms.length; a++) {
            Branch<I> arm = new Branch<>(null, null);
            arm.choiceArm = true;
            arms[a].apply(arm);
            if (arm.guard == null && !arm.otherwise) {
                throw new IllegalArgumentException("arm " + a + " of thenOneOf must open with when(...) or otherwise()");
            }
            if (arm.otherwise && a != arms.length - 1) {
                throw new IllegalArgumentException("otherwise() must be the last arm of thenOneOf");
            }
            cases.add(new BranchStep.Case(arm.otherwise ? null : arm.guard, arm.steps));
        }
        if (cases.isEmpty() || cases.getFirst().guard() == null) {
            throw new IllegalArgumentException("thenOneOf needs an arm opened with when(...)");
        }
        steps.add(BranchStep.choice(cases));
        return (Branch<R>) this;
    }

    /** Opens an arm of {@link #thenOneOf}: the steps chained after it run when {@code guard} holds. */
    public Branch<I> when(FlowGate<? super I> guard) {
        requireArmStart("when");
        this.guard = StepNames.ofBranchStep(guard);
        return this;
    }

    /** Opens the last arm of {@link #thenOneOf}, run when no guard held. */
    public Branch<I> otherwise() {
        requireArmStart("otherwise");
        this.otherwise = true;
        return this;
    }

    private void requireArmStart(String what) {
        if (!choiceArm || !steps.isEmpty() || guard != null || otherwise) {
            throw new IllegalStateException(what + "(...) opens an arm of thenOneOf, so it comes first in one");
        }
    }

    /** The steps {@code body} chains onto a fresh branch: an arm or an escalation. */
    private List<BranchStep> chain(UnaryOperator<Branch<I>> body, String what) {
        Branch<I> sub = new Branch<>(null, null);
        body.apply(sub);
        if (sub.steps.isEmpty()) throw new IllegalArgumentException(what + " has no steps");
        return List.copyOf(sub.steps);
    }

    public Branch<I> thenSleep(Duration duration) {
        if (duration.isNegative()) throw new IllegalArgumentException("a sleep cannot be negative: " + duration);
        steps.add(BranchStep.sleep(duration.toMillis()));
        return this;
    }

    @SuppressWarnings("unchecked")
    private <R> Branch<R> add(String name, NodeKind kind, boolean compensable, RetryPolicy retry, String queue) {
        steps.add(BranchStep.step(name, kind, compensable, queue, retry));
        return (Branch<R>) this;
    }
}
