package com.wiggle.server.engine;

import com.wiggle.server.engine.WorkflowEngine.StepInput;
import com.wiggle.server.store.Tx;

import java.util.List;

/**
 * An ordered run of steps an instrumented application already executed, reported after the fact.
 * {@code task} is the locked instance and its one held token (null once the instance has ended);
 * {@code reporter} names the observing process; {@code fin} says the run is over, so an instance
 * still running afterwards never reached END.
 */
public record ObserveRunContext(Tokens.LockedTask task, String reporter, List<StepInput> steps,
                                boolean fin, Tx tx, long loopMaxIterations) {
}
