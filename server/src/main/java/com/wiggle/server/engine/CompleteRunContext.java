package com.wiggle.server.engine;

import com.wiggle.core.EmittedEvent;
import com.wiggle.server.store.Tx;

import java.util.List;

/**
 * One worker-reported task completion, with the instance already locked by the engine.
 * {@code startedAt}/{@code finishedAt} are the handler's own clock when the worker reported one,
 * else null and the server's stamps stand; {@code events} are what the handler emitted while it
 * ran, appended to the event log in this same transaction.
 */
public record CompleteRunContext (
        Tokens.LockedTask task, String leaseOwner, Object result, Tx tx, long loopMaxIterations,
        Long startedAt, Long finishedAt, List<EmittedEvent> events) implements RunContext{

    public CompleteRunContext {
        events = events == null ? List.of() : List.copyOf(events);
    }
}
