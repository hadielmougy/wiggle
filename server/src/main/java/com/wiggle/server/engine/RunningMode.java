package com.wiggle.server.engine;

import com.wiggle.core.ExecutionMode;
import com.wiggle.server.engine.WorkflowEngine.AdvanceOutcome;

/**
 * How a workflow's steps are executed and reported. Each mode owns its own procedure: the
 * engine locks the instance, resolves the mode, and hands off. The two operations mirror the
 * engine's public surface -- {@code complete} for a single worker-reported result,
 * {@code advance} for a run of locally-chained steps.
 */
public interface RunningMode {

    /** Applies one worker-reported task result and drives the continuation. */
    void complete(CompleteRunContext ctx);

    /** Applies an ordered run of locally-executed steps under a single instance lock. */
    AdvanceOutcome advance(AdvanceRunContext ctx);

    /** The mode a definition actually runs under; DEFAULT and an absent mode both mean SERVER. */
    static ExecutionMode resolveMode(ExecutionMode mode) {
        return mode == null || mode == ExecutionMode.DEFAULT ? ExecutionMode.SERVER : mode;
    }
}
