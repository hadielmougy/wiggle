package com.wiggle.server.engine;

import com.wiggle.server.store.Tx;

/**
 * One worker-reported task completion, with the instance already locked by the engine.
 * {@code startedAt}/{@code finishedAt} are the handler's own clock when the worker reported one,
 * else null and the server's stamps stand.
 */
public record CompleteRunContext(
        Tokens.LockedTask task, String leaseOwner, Object result, Tx tx, long loopMaxIterations,
        Long startedAt, Long finishedAt) {

    public CompleteRunContext(Tokens.LockedTask task, String leaseOwner, Object result, Tx tx, long loopMaxIterations) {
        this(task, leaseOwner, result, tx, loopMaxIterations, null, null);
    }
}
