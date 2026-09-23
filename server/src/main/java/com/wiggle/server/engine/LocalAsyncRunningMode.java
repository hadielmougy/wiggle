package com.wiggle.server.engine;

import com.wiggle.core.ExecutionMode;
import com.wiggle.server.engine.WorkflowEngine.AdvanceOutcome;
import com.wiggle.server.engine.WorkflowEngine.Run;
import com.wiggle.server.engine.WorkflowEngine.RunResult;
import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.Rows.Token;
import com.wiggle.server.store.BufferedTx;
import com.wiggle.server.store.Tx;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;


public class LocalAsyncRunningMode extends BaseRunningMode {

    LocalAsyncRunningMode(Instances instances, NodeBehaviourFactory nodeBehaviourFactory, DefinitionRegistry definitions) {
        super(instances, nodeBehaviourFactory, definitions);
    }

    @Override
    public Map<String, RunResult> advanceMany(AdvanceBatchContext ctx) {
        Tx tx = ctx.tx();
        Map<String, RunResult> results = new LinkedHashMap<>();

        Map<String, Token> probes = byId(tx.findTokens(
                ctx.runs().stream().map(Run::startTaskId).toList()));
        List<Run> probed = new ArrayList<>();
        Map<String, String> instanceOf = new HashMap<>();
        Set<String> owned = new HashSet<>();
        for (Run run : ctx.runs()) {
            Token probe = probes.get(run.startTaskId());
            if (probe == null) {
                results.put(run.startTaskId(), RunResult.reject(EngineException.notFound("task")));
            } else if (!owned.add(probe.instanceId)) {
                results.put(run.startTaskId(), RunResult.reject(EngineException.conflict(
                        "another run in this batch already advances instance " + probe.instanceId
                                + "; report this run again once it lands")));
            } else {
                instanceOf.put(run.startTaskId(), probe.instanceId);
                probed.add(run);
            }
        }

        Map<String, Instance> locked = new HashMap<>();
        for (Instance inst : tx.lockInstances(List.copyOf(new TreeSet<>(instanceOf.values())))) {
            locked.put(inst.id, inst);
        }
        Map<String, Token> tokens = byId(tx.findTokens(
                probed.stream().map(Run::startTaskId).toList()));

        List<Run> survivors = new ArrayList<>();
        Map<String, Tokens.LockedTask> tasks = new HashMap<>();
        for (Run run : probed) {
            Instance inst = locked.get(instanceOf.get(run.startTaskId()));
            Token t = inst == null ? null : tokens.get(run.startTaskId());
            RunResult refusal = validate(tx, inst, t, run);
            if (refusal != null) {
                results.put(run.startTaskId(), refusal);
            } else {
                tasks.put(run.startTaskId(), new Tokens.LockedTask(inst, t));
                survivors.add(run);
            }
        }
        Tx applyTx = tx.transactional() ? BufferedTx.of(tx) : tx;
        for (Run run : survivors) {
            results.put(run.startTaskId(), RunResult.of(chainSteps(new AdvanceRunContext(
                    tasks.get(run.startTaskId()), run.leaseOwner(), run.steps(), run.finalHandback(),
                    applyTx, ctx.loopMaxIterations(), ctx.leaseMillis()))));
        }
        if (applyTx instanceof BufferedTx buffered) buffered.flush();
        return results;
    }

    private static Map<String, Token> byId(List<Token> rows) {
        Map<String, Token> out = new HashMap<>(rows.size() * 2);
        for (Token t : rows) out.put(t.id, t);
        return out;
    }

    private RunResult validate(Tx tx, Instance inst, Token t, Run run) {
        if (inst == null) {
            return RunResult.reject(EngineException.conflict("instance of task " + run.startTaskId()
                    + " is held by a concurrent operation or gone -- report this run singly"));
        }
        if (t == null) {
            return RunResult.reject(EngineException.notFound("task"));
        }
        if (!InstanceState.of(inst.status).running()) {
            return RunResult.of(new AdvanceOutcome(inst.status.name(), 0, null));
        }
        try {
            Tokens.requireLease(t, run.leaseOwner());
            requireMatchingNode(t, run.steps().getFirst());
        } catch (EngineException e) {
            return RunResult.reject(e);
        }
        if (inst.parentTokenId != null) {
            return RunResult.reject(EngineException.conflict("instance " + inst.id
                    + " is a sub-workflow of another instance -- report this run singly"));
        }
        ExecutionMode mode = RunningMode.resolveMode(
                definitions().executionMode(tx, inst.workflow, inst.version));
        if (mode != ExecutionMode.LOCAL_ASYNC) {
            return RunResult.reject(EngineException.conflict("definition " + inst.workflow
                    + ":" + inst.version + " runs " + mode
                    + "; a batch takes only LOCAL_ASYNC runs -- report this run singly"));
        }
        return null;
    }
}
