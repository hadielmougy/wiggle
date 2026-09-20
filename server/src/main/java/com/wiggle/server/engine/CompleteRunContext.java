package com.wiggle.server.engine;

import com.wiggle.server.store.Tx;

/** One worker-reported task completion, with the instance already task by the engine. */
public record CompleteRunContext(
        Tokens.LockedTask task, String leaseOwner, Object result, Tx tx, long loopMaxIterations) {
}
