package com.wiggle.server.engine;

import com.wiggle.core.ExecutionMode;
import com.wiggle.core.Ids;
import com.wiggle.core.Node;
import com.wiggle.core.NodeKind;
import com.wiggle.core.ObserveResult;
import com.wiggle.core.WorkflowDefinition;
import com.wiggle.server.engine.WorkflowEngine.AdvanceOutcome;
import com.wiggle.server.engine.WorkflowEngine.StepInput;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.Rows.Token;
import com.wiggle.server.store.Tx;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Optional;

/**
 * The steps ran inside an instrumented application and arrive after the fact, so the server
 * never dispatches, never leases to a worker, and never refuses a report: a step that departs
 * from the topology is recorded as an anomaly and the run resynchronised to where the report
 * says it is, so the rest of the run still yields its timings. The instance's one token is held
 * by the reporter with a lease no sweeper reclaims, which is what keeps it out of every poll.
 *
 * <p>Only END leaves the worker-reported kinds, and only END is ever driven: an observed graph
 * is restricted at registration to TASK, PREDICATE and END, so the server-side pump has nothing
 * else to run for something that already happened.
 */
public class ObservedRunningMode extends BaseRunningMode {

    /** A lease no sweeper ever reclaims: the holder is a reporting process, not a worker. */
    static final long NO_EXPIRY = Long.MAX_VALUE;

    static final String OUT_OF_ORDER = "OUT_OF_ORDER";
    static final String UNKNOWN_NODE = "UNKNOWN_NODE";
    static final String AFTER_END = "AFTER_END";
    static final String INCOMPLETE = "INCOMPLETE";

    ObservedRunningMode(Instances instances, NodeBehaviourFactory nodeBehaviourFactory, DefinitionRegistry definitions) {
        super(instances, nodeBehaviourFactory, definitions);
    }

    @Override
    public void complete(CompleteRunContext ctx) {
        completeStep(ctx);
    }

    @Override
    public AdvanceOutcome advance(AdvanceRunContext ctx) {
        return chainSteps(ctx);
    }

    /** What an observed graph may contain; anything else needs the server to run it. */
    static void requireObservable(WorkflowDefinition def) {
        for (Node n : def.nodes().values()) {
            if (n.kind() != NodeKind.TASK && n.kind() != NodeKind.PREDICATE && n.kind() != NodeKind.END) {
                throw EngineException.badRequest("workflow '" + def.key() + "' is OBSERVED but has a "
                        + n.kind() + " node '" + n.name() + "'; an observed graph holds only steps, "
                        + "predicates and ends");
            }
            if (n.compensable()) {
                throw EngineException.badRequest("workflow '" + def.key() + "' is OBSERVED but step '"
                        + n.name() + "' is compensable; nothing runs a compensator for an observed run");
            }
        }
    }

    static void requireObserved(ExecutionMode mode, String key) {
        if (RunningMode.resolveMode(mode) != ExecutionMode.OBSERVED) {
            throw EngineException.badRequest("workflow '" + key + "' runs " + RunningMode.resolveMode(mode)
                    + ", not OBSERVED; its steps are reported by workers, not observed");
        }
    }

    /**
     * Applies the reported steps in order under the instance lock, recording every departure from
     * the topology instead of refusing it. The reply carries the instance's status afterwards and
     * how many anomalies this report added.
     */
    ObserveResult observe(ObserveRunContext ctx) {
        Tx tx = ctx.tx();
        Instance inst = ctx.task().inst();
        Token current = ctx.task().token();
        long now = System.currentTimeMillis();
        LazyGraph def = definitions().graph(tx, inst.workflow, inst.version);
        int anomalies = 0;
        for (StepInput step : ctx.steps()) {
            if (!InstanceState.of(inst.status).running()) {
                record(tx, inst, AFTER_END, null, step.nodeId(),
                        "steps reported after the instance " + inst.status, now);
                anomalies++;
                break;
            }
            if (current == null) {
                throw EngineException.conflict("instance " + inst.id + " is running but holds no token");
            }
            Optional<Node> reported = def.find(step.nodeId()).filter(Node::isWorkerDispatched);
            if (reported.isEmpty()) {
                record(tx, inst, UNKNOWN_NODE, current.nodeId, step.nodeId(),
                        "no step '" + step.nodeId() + "' in " + def.key(), now);
                anomalies++;
                continue;
            }
            Node node = def.node(current.nodeId);
            if (!node.id().equals(step.nodeId())) {
                record(tx, inst, OUT_OF_ORDER, node.id(), step.nodeId(),
                        "expected " + node.name() + ", got " + reported.get().name(), now);
                anomalies++;
                Tokens.cancel(tx, current, now);
                node = reported.get();
                current = hold(tx, inst, Tokens.create(inst, node.id(), "", null, now), node, ctx.reporter(), now);
            }
            stamp(current, step);
            if (step.error() != null) {
                Tokens.reportFailure(tx, current, node, step.error(), step.error(), false, now);
                instances().fail(tx, inst, node.name() + ": " + step.error(), now);
                continue;
            }
            NodeBehaviour behaviour = behaviours().getNodeBehaviour(node.kind());
            StepReport report = StepReport.of(step);
            String next = behaviour.route(inst, current, node, report);
            Overrun overrun = behaviour.overrun(current, node, report, ctx.loopMaxIterations());
            if (overrun.exceeded()) {
                Tokens.settle(tx, current, now);
                instances().fail(tx, inst, overrun.message(), now);
                continue;
            }
            Tokens.settle(tx, current, now);
            Node nextNode = def.node(next);
            Token cont = Tokens.create(inst, next, current.joinStack,
                    Scopes.stripCombineScratch(node, current.payload), now);
            if (nextNode.isWorkerDispatched()) {
                current = hold(tx, inst, cont, nextNode, ctx.reporter(), now);
            } else {
                tx.insertToken(cont);
                Drive.pump(behaviours(), tx, def, inst, new ArrayDeque<>(List.of(cont)), now);
                current = null;
            }
        }
        if (ctx.fin() && InstanceState.of(inst.status).running()) {
            String at = current == null ? "?" : def.node(current.nodeId).name();
            record(tx, inst, INCOMPLETE, current == null ? null : current.nodeId, null,
                    "run ended before END, at " + at, now);
            anomalies++;
            instances().fail(tx, inst, "run ended before END, at " + at, now);
        } else if (InstanceState.of(inst.status).running()) {
            Instances.touch(tx, inst, now);
        }
        return new ObserveResult(inst.id, inst.status.name(), anomalies);
    }

    private static Token hold(Tx tx, Instance inst, Token t, Node node, String reporter, long now) {
        Tokens.createLeased(tx, t, node, reporter, NO_EXPIRY, now);
        return t;
    }

    private static void record(Tx tx, Instance inst, String kind, String expected, String reported,
                               String detail, long now) {
        tx.insertAnomaly(new Rows.Anomaly(Ids.next("anm"), inst.id, inst.workflow, inst.version,
                kind, expected, reported, detail, now));
    }
}
