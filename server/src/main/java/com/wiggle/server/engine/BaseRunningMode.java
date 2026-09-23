package com.wiggle.server.engine;

import com.wiggle.core.Doc;
import com.wiggle.core.Node;
import com.wiggle.core.ObserveResult;
import com.wiggle.server.engine.WorkflowEngine.AdvanceOutcome;
import com.wiggle.server.engine.WorkflowEngine.RunResult;
import com.wiggle.server.engine.WorkflowEngine.StepInput;
import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.Rows.Token;
import com.wiggle.server.store.Tx;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;

/**
 * The step machinery every {@link RunningMode} is built from. Every mode reaches this through the
 * same {@code execute}, one implementation for all of them: {@link ExecutionContext#runOn} decides
 * which procedure runs, not a switch. {@link #completeStep}/{@link #chainSteps} are genuinely
 * shared and every mode gets them; {@link #advanceMany}/{@link #observe}/{@link #settle} are
 * mode-specific and default to refusing the call, overridden only by the one mode that supports
 * each (LOCAL_ASYNC, OBSERVED, OBSERVED respectively) -- the same shape {@code advanceMany} always
 * had as a default interface method, now reached by context dispatch instead.
 */
abstract class BaseRunningMode implements RunningMode {

    private static final System.Logger LOG = System.getLogger(BaseRunningMode.class.getName());

    private final NodeBehaviourFactory nodeBehaviourFactory;
    private final DefinitionRegistry definitions;
    private final Instances instances;

    BaseRunningMode(Instances instances, NodeBehaviourFactory nodeBehaviourFactory, DefinitionRegistry definitions) {
        this.instances = instances;
        this.nodeBehaviourFactory = nodeBehaviourFactory;
        this.definitions = definitions;
    }

    @Override
    public final <T> T execute(ExecutionContext<T> ctx) {
        return ctx.runOn(this);
    }

    /** Applies a cross-instance batch under one transaction. Only {@link LocalAsyncRunningMode} does this. */
    Map<String, RunResult> advanceMany(AdvanceBatchContext ctx) {
        throw new UnsupportedOperationException("advanceMany is not supported by this mode");
    }

    /** Appends a run an instrumented application already executed. Only {@link ObservedRunningMode} does this. */
    ObserveResult observe(ObserveRunContext ctx) {
        throw new UnsupportedOperationException("observe is not supported by this mode");
    }

    /** Judges one settled observed run. Only {@link ObservedRunningMode} does this. */
    void settle(SettleContext ctx) {
        throw new UnsupportedOperationException("settle is not supported by this mode");
    }

    final DefinitionRegistry definitions() {
        return definitions;
    }

    final Instances instances() {
        return instances;
    }

    /** The step's own clock, when its reporter sent one; a step reported untimed keeps the
     *  server's stamps (claimed, then settled). */
    static void stamp(Token t, StepInput step) {
        if (step.startedAt() == null || step.finishedAt() == null) return;
        t.startedAt = step.startedAt();
        t.finishedAt = step.finishedAt();
    }

    /** Applies one reported result to the task token, then drives the continuation to its park. */
    final void completeStep(CompleteExecutionContext ctx) {
        Tx tx           = ctx.tx();
        Instance inst   = ctx.task().inst();
        Token t         = ctx.task().token();
        Tokens.requireLease(t, ctx.leaseOwner());
        long now = System.currentTimeMillis();
        Instances.requireRunning(inst);
        LazyGraph def = definitions.graph(tx, t.workflow, t.version);
        Node node = def.node(t.nodeId);
        Events.emitted(tx, inst, node.id(), ctx.events(), now);   // committed with the settle below, or not at all
        Doc compInput = node.compensable() ? Scopes.dispatchContext(inst, t) : null;
        NodeBehaviour behaviour = nodeBehaviourFactory.getNodeBehaviour(node.kind());
        if (ctx.startedAt() != null && ctx.finishedAt() != null) {   // the handler's clock beats claim-to-settle
            t.startedAt = ctx.startedAt();
            t.finishedAt = ctx.finishedAt();
        }
        StepReport report = StepReport.of(ctx.result());
        String next = behaviour.route(inst, t, node, report);
        if (node.compensable()) Sagas.capture(tx, inst, t, node, compInput, now);
        Overrun overrun = behaviour.overrun(t, node, report, ctx.loopMaxIterations());
        if (overrun.exceeded()) {
            Tokens.settle(tx, t, now);
            instances.fail(tx, inst, overrun.message(), now);
            return;
        }
        Tokens.settle(tx, t, now);
        Instances.touch(tx, inst, now);
        Token cont = Tokens.continueAt(tx, inst, t, next, Scopes.stripCombineScratch(node, t.payload), now);
        drive(tx, def, inst, cont, now);
    }

    /**
     * Applies the reported steps in order under the one lock. Between steps the continuation is
     * leased straight back to the same worker (never exposed to {@code poll}); at the final step,
     * at a handback, or at a node the worker cannot run, it is driven normally and the worker
     * released.
     */
    final AdvanceOutcome chainSteps(AdvanceRunContext ctx) {
        Tx tx = ctx.tx();
        Instance inst = ctx.task().inst();
        long now = System.currentTimeMillis();
        long leaseExpiry = now + ctx.leaseMillis();
        if (!InstanceState.of(inst.status).running()) {
            return new AdvanceOutcome(inst.status.name(), 0, null);
        }
        Token current = ctx.task().token();
        Tokens.requireLease(current, ctx.leaseOwner());
        LazyGraph def = definitions.graph(tx, inst.workflow, inst.version);
        List<StepInput> steps = ctx.steps();
        String nextTaskId = null;
        for (int i = 0; i < steps.size(); i++) {
            StepInput step = steps.get(i);
            Node node = def.node(current.nodeId);
            requireMatchingNode(current, step);
            Events.emitted(tx, inst, node.id(), step.events(), now);   // committed with this step, or not at all
            // A step flushed together with others ran somewhere inside the batch: the server's
            // stamps would say it took no time at all, so without the worker's own clock it is untimed.
            if (steps.size() > 1 && (step.startedAt() == null || step.finishedAt() == null)) {
                current.startedAt = null;
                current.finishedAt = null;
            }
            stamp(current, step);
            NodeBehaviour behaviour = nodeBehaviourFactory.getNodeBehaviour(node.kind());
            Doc compInput = node.compensable() ? Scopes.dispatchContext(inst, current) : null;
            StepReport report = StepReport.of(step);
            String next = behaviour.route(inst, current, node, report);
            if (node.compensable()) Sagas.capture(tx, inst, current, node, compInput, now);
            Overrun overrun = behaviour.overrun(current, node, report, ctx.loopMaxIterations());
            if (overrun.exceeded()) {
                Tokens.settle(tx, current, now);
                instances.fail(tx, inst, overrun.message(), now);
                return new AdvanceOutcome(inst.status.name(), 0, null);
            }
            Tokens.settle(tx, current, now);
            Token cont = Tokens.create(inst, next, current.joinStack,
                    Scopes.stripCombineScratch(node, current.payload), now);
            Node nextNode = def.node(next);
            boolean lastStep = i == steps.size() - 1;
            if ((lastStep && ctx.finalHandback()) || !nextNode.isWorkerDispatched()) {
                Instances.touch(tx, inst, now);
                handBack(tx, def, inst, cont, nextNode, now);
                return new AdvanceOutcome(inst.status.name(), leaseExpiry, null);
            }
            Tokens.createLeased(tx, cont, nextNode, ctx.leaseOwner(), leaseExpiry, now);
            LOG.log(System.Logger.Level.DEBUG, () -> "advanceRun: instance " + inst.id
                    + " chaining locally " + node.name() + " -> " + next);
            current = cont;
            nextTaskId = cont.id;
        }
        Instances.touch(tx, inst, now);
        return new AdvanceOutcome(inst.status.name(), leaseExpiry, nextTaskId);
    }

    /** A reported step must name the node its token is actually at; shared with batch validate. */
    static void requireMatchingNode(Token current, StepInput step) {
        if (!current.nodeId.equals(step.nodeId())) {
            throw EngineException.conflict("reported step " + step.nodeId() + " but token "
                    + current.id + " is at " + current.nodeId);
        }
    }

    /** Hand back: drive the continuation normally (READY for a worker, or a boundary). */
    private void handBack(Tx tx, LazyGraph def, Instance inst, Token cont, Node nextNode, long now) {
        tx.insertToken(cont);
        LOG.log(System.Logger.Level.DEBUG, () -> "advanceRun: instance " + inst.id
                + " handing back at " + cont.nodeId + " (" + nextNode.kind() + ")");
        drive(tx, def, inst, cont, now);
    }

    private void drive(Tx tx, LazyGraph def, Instance inst, Token cont, long now) {
        Drive.pump(nodeBehaviourFactory, tx, def, inst, new ArrayDeque<>(List.of(cont)), now);
    }
}
