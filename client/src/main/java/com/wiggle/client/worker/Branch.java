package com.wiggle.client.worker;

import com.wiggle.client.flow.FlowEffect;
import com.wiggle.client.flow.FlowFactory;
import com.wiggle.client.flow.FlowFn;
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
