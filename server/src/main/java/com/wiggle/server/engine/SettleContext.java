package com.wiggle.server.engine;

import com.wiggle.core.WorkflowDefinition;
import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.Tx;

/**
 * Judges one settled observed run: {@code idle} says it settled by going quiet rather than by
 * reaching END or being reported final.
 */
public record SettleContext(Tx tx, Instance inst, WorkflowDefinition def, boolean idle, long now)
        implements ExecutionContext<Void> {

    @Override
    public Void runOn(BaseRunningMode mode) {
        mode.settle(this);
        return null;
    }
}
