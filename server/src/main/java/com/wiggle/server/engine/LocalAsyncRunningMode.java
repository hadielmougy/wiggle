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
    public void complete(CompleteRunContext ctx) {
        completeStep(ctx);
    }

    @Override
    public AdvanceOutcome advance(AdvanceRunContext ctx) {
        return chainSteps(ctx);
    }

    /**
     * Applies N independent single-instance runs under one transaction: validate, then apply.
     *
     * <p>Validate locks every instance in sorted id order -- two workers with overlapping batches
     * take the same locks in the same order and cannot deadlock, which also needs every lock the
     * apply phase will ever take to be in that sorted set: sub-workflow children are rejected
     * below precisely because completing one locks its parent outside it -- then checks, per run, everything
     * the single-run path refuses before its first write: the task exists, the lease is held, the
     * token is not a compensator, the first step matches the token's node, the definition resolves
     * to LOCAL_ASYNC, and no earlier run in the batch owns the same instance. A compensator can
     * never survive validation: it only exists while its instance is COMPENSATING, which the
     * running check answers with the status outcome, exactly as the single-run path does. A run that fails any
     * of these is answered in the result map and dropped; nothing was written on its behalf, so a
     * bad run costs its batch-mates nothing.
     *
     * <p>Apply is {@link #chainSteps} per survivor, unchanged: a loop overrun fails that instance
     * durably and its result rides in the same commit as everyone else's. What apply may still
     * throw -- a later step's node mismatch, a storage failure -- rolls the whole batch back, and
     * the engine replays each run in its own transaction.
     */
    Map<String, RunResult> advanceMany(AdvanceBatchContext ctx) {
        Tx tx = ctx.tx();
        Map<String, RunResult> results = new LinkedHashMap<>();

        // Probe without locking, one read for the whole batch: which instance does each run
        // belong to? Two runs on one instance would interleave writes to the same context, the
        // second silently clobbering the first -- and one poll can hand a worker two arms of the
        // same fork -- so only the first stays.
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

        // One statement locks every instance, ids ascending, and one re-read fetches the tokens
        // as they are under those locks -- the probe rows above are stale by definition.
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

        // Apply runs on a write-buffering view: the settles and leased continuations of the whole
        // batch reach the store as executeBatch groups instead of a round-trip apiece, and any
        // read a step makes flushes first, so nothing behaves differently -- it just travels
        // together. The flush before returning is what makes the buffered writes part of the
        // commit at all.
        BufferedTx buffered = BufferedTx.of(tx);
        for (Run run : survivors) {
            results.put(run.startTaskId(), RunResult.of(chainSteps(new AdvanceRunContext(
                    tasks.get(run.startTaskId()), run.leaseOwner(), run.steps(), run.finalHandback(),
                    buffered, ctx.loopMaxIterations(), ctx.leaseMillis()))));
        }
        buffered.flush();
        return results;
    }

    private static Map<String, Token> byId(List<Token> rows) {
        Map<String, Token> out = new HashMap<>(rows.size() * 2);
        for (Token t : rows) out.put(t.id, t);
        return out;
    }

    /** The single-run path's pre-write refusals, as a result instead of a throw; null passes. */
    private RunResult validate(Tx tx, Instance inst, Token t, Run run) {
        if (inst == null || t == null) {
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
        // A sub-workflow child completing (or failing) locks its parent chain mid-apply --
        // Instances.notifyParent -- which the sorted lock order above cannot cover: a second
        // batch holding the parent and wanting this child would deadlock. Children take the
        // single-run path, whose one-direct-lock-plus-ancestors shape has no cycles.
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
