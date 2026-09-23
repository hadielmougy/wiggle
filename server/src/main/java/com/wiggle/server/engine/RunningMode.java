package com.wiggle.server.engine;

import com.wiggle.core.ExecutionMode;
import com.wiggle.server.engine.WorkflowEngine.AdvanceOutcome;

import java.util.Map;

/**
 * How a workflow's steps are executed and reported. Each mode owns its own procedure: the
 * engine locks the instance, resolves the mode, and hands off. The two operations mirror the
 * engine's public surface -- {@code complete} for a single worker-reported result,
 * {@code advance} for a run of locally-chained steps.
 */
public interface RunningMode {

    void complete(CompleteRunContext ctx);

    AdvanceOutcome advance(AdvanceRunContext ctx);

    default Map<String, WorkflowEngine.RunResult> advanceMany(AdvanceBatchContext ctx) {
        throw new UnsupportedOperationException("advanceMany is not supported by this mode");
    }

    /** The mode a definition actually runs under; DEFAULT and an absent mode both mean SERVER. */
    static ExecutionMode resolveMode(ExecutionMode mode) {
        return mode == null || mode == ExecutionMode.DEFAULT ? ExecutionMode.SERVER : mode;
    }
}
