package com.wiggle.server.engine;

import com.wiggle.core.Doc;
import com.wiggle.core.Node;
import com.wiggle.core.NodeKind;
import com.wiggle.server.engine.WorkflowEngine.Run;
import com.wiggle.server.engine.WorkflowEngine.StepInput;
import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.Rows.LockedTask;
import com.wiggle.server.store.Rows.Token;
import com.wiggle.server.store.Tx;

import java.util.ArrayDeque;
import java.util.List;

/**
 * Applies a worker-reported run, step by step under the instance's one lock. The same procedure
 * serves every worker-run mode; they differ only in {@code chainsBack}, whether the continuation
 * may be leased back to the reporting worker instead of released (see
 * {@link ExecutionModes#chainsBack}).
 */
final class StepChain {

    private static final System.Logger LOG = System.getLogger(StepChain.class.getName());

    private final NodeBehaviourFactory nodeBehaviourFactory;
    private final DefinitionRegistry definitions;
    private final Instances instances;
    private final long loopMaxIterations;
    private final long leaseMillis;

    StepChain(Instances instances, NodeBehaviourFactory nodeBehaviourFactory, DefinitionRegistry definitions,
              long loopMaxIterations, long leaseMillis) {
        this.instances = instances;
        this.nodeBehaviourFactory = nodeBehaviourFactory;
        this.definitions = definitions;
        this.loopMaxIterations = loopMaxIterations;
        this.leaseMillis = leaseMillis;
    }

    /**
     * Applies the reported steps in order under the one lock. Where the mode chains, the
     * continuation between steps is leased straight back to the same worker (never exposed to
     * {@code poll}); at the final step, at a handback, at a node the worker cannot run, or when
     * {@code chainsBack} is false, it is driven normally and the worker released.
     */
    ReportOutcome apply(Tx tx, LockedTask task, Run run, boolean chainsBack) {
        Instance inst = task.inst();
        long now = System.currentTimeMillis();
        long leaseExpiry = now + leaseMillis;
        if (!inst.status.running()) return ReportOutcome.stopped(inst);
        Token current = task.token();
        Tokens.requireLease(current, run.leaseOwner());
        LazyGraph def = definitions.graph(tx, inst.workflow, inst.version);
        List<StepInput> steps = run.steps();
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
            Doc input = node.compensable() || StepIo.ENABLED ? Scopes.dispatchContext(inst, current) : null;
            Doc compInput = node.compensable() ? input : null;
            StepReport report = StepReport.of(step);
            String next = behaviour.route(inst, current, node, report);
            StepIo.record(current, input, node.kind() == NodeKind.PREDICATE ? step.predicateValue() : step.merge());
            if (node.compensable()) Sagas.capture(tx, inst, current, node, compInput, now);
            Overrun overrun = behaviour.overrun(current, node, report, loopMaxIterations);
            if (overrun.exceeded()) {
                Tokens.settle(tx, current, now);
                instances.fail(tx, inst, overrun.message(), now);
                return new ReportOutcome(inst.status.name(), 0, null);
            }
            Tokens.settle(tx, current, now);
            Token cont = Tokens.create(inst, next, current.joinStack,
                    Scopes.stripCombineScratch(node, current.payload), now);
            Node nextNode = def.node(next);
            // The last step ends the run unless this worker both may and wants to carry on.
            boolean lastStep = i == steps.size() - 1;
            if ((lastStep && (run.finalHandback() || !chainsBack)) || !nextNode.isWorkerDispatched()) {
                Instances.touch(tx, inst, now);
                handBack(tx, def, inst, cont, nextNode, now);
                return new ReportOutcome(inst.status.name(), leaseExpiry, null);
            }
            Tokens.createLeased(tx, cont, nextNode, run.leaseOwner(), leaseExpiry, now);
            LOG.log(System.Logger.Level.DEBUG, () -> "reportSteps: instance " + inst.id
                    + " chaining locally " + node.name() + " -> " + next);
            current = cont;
            nextTaskId = cont.id;
        }
        Instances.touch(tx, inst, now);
        return new ReportOutcome(inst.status.name(), leaseExpiry, nextTaskId);
    }

    /** Hand back: drive the continuation normally (READY for a worker, or a boundary). */
    private void handBack(Tx tx, LazyGraph def, Instance inst, Token cont, Node nextNode, long now) {
        tx.insertToken(cont);
        LOG.log(System.Logger.Level.DEBUG, () -> "reportSteps: instance " + inst.id
                + " handing back at " + cont.nodeId + " (" + nextNode.kind() + ")");
        drive(tx, def, inst, cont, now);
    }

    /** The step's own clock, when its reporter sent one; a step reported untimed keeps the
     *  server's stamps (claimed, then settled). */
    static void stamp(Token t, StepInput step) {
        if (step.startedAt() == null || step.finishedAt() == null) return;
        t.startedAt = step.startedAt();
        t.finishedAt = step.finishedAt();
    }

    /** A reported step must name the node its token is actually at. */
    static void requireMatchingNode(Token current, StepInput step) {
        if (!current.nodeId.equals(step.nodeId())) {
            throw EngineException.conflict("reported step " + step.nodeId() + " but token "
                    + current.id + " is at " + current.nodeId);
        }
    }

    private void drive(Tx tx, LazyGraph def, Instance inst, Token cont, long now) {
        Drive.pump(nodeBehaviourFactory, tx, def, inst, new ArrayDeque<>(List.of(cont)), now);
    }
}
