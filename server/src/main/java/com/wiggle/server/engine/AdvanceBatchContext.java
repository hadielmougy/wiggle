package com.wiggle.server.engine;

import com.wiggle.server.engine.WorkflowEngine.Run;
import com.wiggle.server.store.Tx;

import java.util.List;

/** A cross-instance batch of runs, applied under one transaction and one commit. */
public record AdvanceBatchContext(List<Run> runs, Tx tx, long loopMaxIterations, long leaseMillis) {
}
