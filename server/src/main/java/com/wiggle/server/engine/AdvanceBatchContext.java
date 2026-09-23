package com.wiggle.server.engine;

import com.wiggle.server.engine.WorkflowEngine.Run;
import com.wiggle.server.engine.WorkflowEngine.RunResult;
import com.wiggle.server.store.Tx;

import java.util.List;
import java.util.Map;

/** A cross-instance batch of runs, applied under one transaction and one commit. */
public record AdvanceBatchContext(List<Run> runs, Tx tx, long loopMaxIterations, long leaseMillis)
        implements ExecutionContext<Map<String, RunResult>> {

    @Override
    public Map<String, RunResult> runOn(BaseRunningMode mode) {
        return mode.advanceMany(this);
    }
}
