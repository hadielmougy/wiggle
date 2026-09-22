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
import com.wiggle.server.store.Rows.InstanceStatus;
import com.wiggle.server.store.Rows.Token;
import com.wiggle.server.store.Rows.TokenStatus;
import com.wiggle.server.store.Tx;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * The steps ran inside instrumented applications and arrive after the fact, from any number of
 * reporters in any order, so the server never dispatches, never holds a token, and never judges a
 * report on arrival. A report only appends: each step becomes a settled, timed token; a step
 * whose successor is END also writes the END token that marks the run as closing; an undo becomes
 * a settled token naming the compensable step it reversed. Judgement is {@link Conformance}, run
 * by the settle sweep once the run has gone quiet.
 *
 * <p>A run that is declared failed -- explicitly, or by a step that threw -- enters the saga's
 * COMPENSATING state when its graph has anything to undo, so the console tells the truth while
 * the services run their undos; the judge then ends it COMPENSATED or COMPENSATION_FAILED. A graph
 * with nothing compensable fails in place.
 *
 * <p>An observed graph is restricted at registration to what the judge understands: steps,
 * predicates, static forks and joins, and ends. Nothing here runs server-side, so nothing else
 * belongs in the graph.
 */
public class ObservedRunningMode extends BaseRunningMode {

    static final String OUT_OF_ORDER = "OUT_OF_ORDER";
    static final String UNKNOWN_NODE = "UNKNOWN_NODE";
    static final String AFTER_END = "AFTER_END";
    static final String INCOMPLETE = "INCOMPLETE";
    static final String DUPLICATE = "DUPLICATE";
    static final String STALLED = "STALLED";
    static final String UNDO_WITHOUT_STEP = "UNDO_WITHOUT_STEP";
    static final String UNDO_WITHOUT_FAILURE = "UNDO_WITHOUT_FAILURE";
    static final String UNDO_OUT_OF_ORDER = "UNDO_OUT_OF_ORDER";
    static final String MISSING_UNDO = "MISSING_UNDO";
    /** A step's recent runs are markedly slower than the runs before them. */
    static final String DEGRADING = "DEGRADING";
    /** Where a reported predicate's value rides on its token, for the judge to read back. */
    static final String PREDICATE_KEY = "__observed.predicate";

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

    /** What an observed graph may contain: what the judge can walk. */
    static void requireObservable(WorkflowDefinition def) {
        for (Node n : def.nodes().values()) {
            switch (n.kind()) {
                case TASK, PREDICATE, FORK, JOIN, END -> { }
                default -> throw EngineException.badRequest("workflow '" + def.key() + "' is OBSERVED but has a "
                        + n.kind() + " node '" + n.name() + "'; an observed graph holds only steps, "
                        + "predicates, forks, joins and ends");
            }
        }
    }

    static void requireObserved(ExecutionMode mode, String key) {
        if (RunningMode.resolveMode(mode) != ExecutionMode.OBSERVED) {
            throw EngineException.badRequest("workflow '" + key + "' runs " + RunningMode.resolveMode(mode)
                    + ", not OBSERVED; its steps are reported by workers, not observed");
        }
    }

    /** Whether an observed instance still takes reports: running, or failed and awaiting its undos. */
    static boolean open(Instance inst) {
        return inst.status == InstanceStatus.RUNNING || inst.status == InstanceStatus.COMPENSATING;
    }

    /**
     * Appends the reported steps to the run and reschedules its judgement. A step the graph does
     * not know is recorded as an anomaly at once; a step reported once the run is terminal is kept
     * for its timing and recorded as AFTER_END; a step that threw, or an explicit failure, declares
     * the run failed, after which its undos are expected.
     */
    ObserveResult observe(ObserveRunContext ctx, long settleMillis, long stallMillis) {
        Tx tx = ctx.tx();
        Instance inst = ctx.inst();
        long now = System.currentTimeMillis();
        LazyGraph def = definitions().graph(tx, inst.workflow, inst.version);
        int anomalies = 0;
        // Closing is sticky: once END was seen (or a report said final) a straggler keeps the short
        // grace rather than pushing the run back out to the stall threshold.
        boolean closing = ctx.fin() || (inst.settleAt != null && inst.settleAt - inst.updatedAt <= settleMillis);
        if (!open(inst) && !ctx.steps().isEmpty()) {
            record(tx, inst, AFTER_END, null, ctx.steps().getFirst().nodeId(),
                    ctx.steps().size() + " step(s) reported after the instance " + inst.status, now);
            anomalies++;
        }
        boolean undoLanded = false;
        // Arrival order within a report is the reporter's causal order: it breaks ties between
        // steps whose clocks agree to the millisecond. Later reports sort after earlier ones.
        long seq = now * 1000;
        for (StepInput step : ctx.steps()) {
            seq++;
            Optional<Node> reported = def.find(step.nodeId()).filter(Node::isWorkerDispatched);
            if (reported.isEmpty()) {
                record(tx, inst, UNKNOWN_NODE, null, step.nodeId(), "no step '" + step.nodeId() + "' in " + def.key(), now);
                anomalies++;
                continue;
            }
            Node node = reported.get();
            if (step.undo()) {
                if (!node.compensable()) {
                    record(tx, inst, UNDO_WITHOUT_STEP, null, node.id(),
                            node.name() + " declares no undo; the topology cannot expect one", now);
                    anomalies++;
                    continue;
                }
                Tokens.insertSettled(tx, inst, node, step.error() != null ? TokenStatus.FAILED : TokenStatus.DONE,
                        ctx.reporter(), step, seq, now);
                undoLanded = true;
                continue;
            }
            if (step.error() != null) {
                Tokens.insertSettled(tx, inst, node, TokenStatus.FAILED, ctx.reporter(), step, seq, now);
                if (inst.status == InstanceStatus.RUNNING) declareFailure(tx, inst, node.name() + ": " + step.error(), now);
                continue;
            }
            Token t = Tokens.insertSettled(tx, inst, node, TokenStatus.DONE, ctx.reporter(), step, seq, now);
            if (step.merge() != null) Scopes.applyStepResult(inst, t, step.merge());
            String next = node.kind() == NodeKind.PREDICATE
                    ? (step.predicateValue() != null && step.predicateValue() ? node.next() : node.altNext())
                    : node.next();
            Node nextNode = next == null ? null : def.find(next).orElse(null);
            if (nextNode != null && nextNode.kind() == NodeKind.END) {
                Tokens.insertSettled(tx, inst, nextNode, TokenStatus.DONE, ctx.reporter(), null, seq, now);
                closing = true;
            }
        }
        if (ctx.failure() != null && inst.status == InstanceStatus.RUNNING) {
            declareFailure(tx, inst, ctx.failure(), now);
        }
        if (inst.status == InstanceStatus.COMPENSATING) {
            // The reverse pass settles once every completed compensable step has its undo; until
            // then the run waits for the services running them, up to the stall threshold.
            inst.settleAt = now + (undoLanded && undosComplete(tx, def, inst) ? settleMillis : stallMillis);
            Instances.touch(tx, inst, now);
        } else if (inst.status == InstanceStatus.RUNNING) {
            inst.settleAt = now + (closing ? settleMillis : stallMillis);
            Instances.touch(tx, inst, now);
        }
        return new ObserveResult(inst.id, inst.status.name(), anomalies);
    }

    /**
     * The run failed. With anything compensable in its graph it enters the reverse pass, awaiting
     * the undos the services run; otherwise it fails in place, as a run with nothing to undo does.
     */
    private void declareFailure(Tx tx, Instance inst, String reason, long now) {
        WorkflowDefinition def = definitions().lookup(inst.workflow, inst.version).orElse(null);
        boolean compensable = def != null && def.nodes().values().stream().anyMatch(Node::compensable);
        if (compensable) instances().compensating(tx, inst, reason, now);
        else instances().fail(tx, inst, reason, now);
    }

    /** Every completed compensable step of the run has an undo reported. */
    private static boolean undosComplete(Tx tx, LazyGraph def, Instance inst) {
        Set<String> completed = new HashSet<>();
        Set<String> undone = new HashSet<>();
        for (Token t : tx.tokensOf(inst.id)) {
            if (t.status != TokenStatus.DONE || t.kind == NodeKind.END) continue;
            if (t.undoOf != null) undone.add(t.undoOf);
            else if (def.find(t.nodeId).map(Node::compensable).orElse(false)) completed.add(t.nodeId);
        }
        return undone.containsAll(completed);
    }

    /**
     * Judges a settled run: sorts its steps by their own clock (arrival order breaks ties), lets
     * {@link Conformance} reorder by causal hints where the graph agrees, writes the findings, and
     * closes the instance. {@code idle} says the run settled by going quiet rather than by reaching
     * END, being reported final, or completing its undos.
     */
    void settle(Tx tx, Instance inst, WorkflowDefinition def, boolean idle, long now) {
        List<Token> tokens = tx.tokensOf(inst.id);
        List<Token> reported = new ArrayList<>();
        for (Token t : tokens) {
            if (!t.isActive() && t.kind != NodeKind.END && t.status != TokenStatus.CANCELLED) reported.add(t);
        }
        reported.sort(Comparator.comparingLong((Token t) -> t.startedAt != null ? t.startedAt : t.createdAt)
                .thenComparingLong(t -> t.seq != null ? t.seq : 0)
                .thenComparing(t -> t.id));
        List<Conformance.Step> steps = new ArrayList<>(reported.size());
        for (Token t : reported) {
            long at = t.startedAt != null ? t.startedAt : t.createdAt;
            long seq = t.seq != null ? t.seq : 0;
            if (t.undoOf != null) {
                steps.add(Conformance.Step.undo(t.undoOf, at, seq, t.status == TokenStatus.FAILED));
            } else {
                Object pv = t.payload.staged().get(PREDICATE_KEY);
                steps.add(new Conformance.Step(t.nodeId, pv instanceof Boolean b ? b : null, at, seq, t.afterNode,
                        null, t.status == TokenStatus.FAILED));
            }
        }
        boolean failed = inst.status == InstanceStatus.COMPENSATING;
        Conformance.Verdict verdict = Conformance.judge(def, steps, failed);
        for (Conformance.Finding f : verdict.findings()) {
            record(tx, inst, f.kind(), f.expected(), f.reported(), f.detail(), now);
        }
        if (idle && !failed && !verdict.reachedEnd()) {
            record(tx, inst, STALLED, verdict.stoppedAt(), null, "no report for " + (now - inst.updatedAt) / 1000 + "s", now);
        }
        if (idle && failed && verdict.compensationError() != null) {
            record(tx, inst, STALLED, null, null, "no report for " + (now - inst.updatedAt) / 1000 + "s with undos outstanding", now);
        }
        inst.settleAt = null;
        if (failed) {
            if (verdict.compensated()) instances().compensated(tx, inst, now);
            else instances().compensationFailed(tx, inst, verdict.compensationError(), now);
        } else if (verdict.completed()) {
            instances().complete(tx, inst, null, now);
        } else if (verdict.endReason() != null) {
            instances().fail(tx, inst, verdict.endReason(), now);
        } else {
            instances().fail(tx, inst, "run ended before END, at " + verdict.stoppedAt(), now);
        }
    }

    private static void record(Tx tx, Instance inst, String kind, String expected, String reported,
                               String detail, long now) {
        tx.insertAnomaly(new Rows.Anomaly(Ids.next("anm"), inst.id, inst.workflow, inst.version,
                kind, expected, reported, detail, now));
    }
}
