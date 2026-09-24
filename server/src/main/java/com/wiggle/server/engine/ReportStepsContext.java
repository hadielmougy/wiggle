package com.wiggle.server.engine;

import com.wiggle.server.engine.WorkflowEngine.StepInput;
import com.wiggle.server.store.Tx;

import java.util.List;

/**
 * The steps one worker reports in a single call: the ordinary case is one, a chaining worker sends
 * the ordered run it executed locally. {@code finalHandback} says this worker will not take a
 * continuation; a mode that never chains hands back regardless.
 */
public record ReportStepsContext(
        Tokens.LockedTask task, String leaseOwner, List<StepInput> steps,
        boolean finalHandback, Tx tx, long loopMaxIterations, long leaseMillis)
        implements ExecutionContext<ReportOutcome> {

    @Override
    public ReportOutcome runOn(BaseRunningMode mode) {
        return mode.chainSteps(this);
    }
}
