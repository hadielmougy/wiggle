package com.wiggle.server.engine;

import com.wiggle.core.ObserveResult;
import com.wiggle.server.engine.WorkflowEngine.StepInput;
import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.Tx;

import java.util.List;

/**
 * A run of steps an instrumented application already executed, reported after the fact against
 * the locked observed instance the run's key maps to. {@code reporter} names the process that
 * ran these steps -- one run may have several. {@code fin} asks for the run to be judged after
 * the short grace rather than the stall threshold. {@code settleMillis}/{@code stallMillis} are
 * the engine's configured windows for the settle sweep this report reschedules.
 */
public record ObserveRunContext(Instance inst, String reporter, List<StepInput> steps, boolean fin, Tx tx,
                                long settleMillis, long stallMillis) implements ExecutionContext<ObserveResult> {

    @Override
    public ObserveResult runOn(BaseRunningMode mode) {
        return mode.observe(this);
    }
}
