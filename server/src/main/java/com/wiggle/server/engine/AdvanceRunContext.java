package com.wiggle.server.engine;

import com.wiggle.server.engine.WorkflowEngine.StepInput;
import com.wiggle.server.store.Tx;

import java.util.List;

/**
 * An ordered run of locally-executed steps reported by one worker, with the instance already
 * task by the engine. {@code finalHandback} means the worker is done after the last step and
 * wants the continuation released rather than leased back.
 */
public record AdvanceRunContext(
        Tokens.LockedTask task, String leaseOwner, List<StepInput> steps,
        boolean finalHandback, Tx tx, long loopMaxIterations, long leaseMillis) {
}
