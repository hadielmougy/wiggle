package com.wiggle.server.engine;

import com.wiggle.server.engine.WorkflowEngine.StepInput;
import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.Tx;

import java.util.List;

/**
 * A run of steps an instrumented application already executed, reported after the fact against
 * the locked observed instance the run's key maps to. {@code reporter} names the process that
 * ran these steps -- one run may have several. {@code fin} asks for the run to be judged after
 * the short grace rather than the stall threshold. {@code failure}, when set, declares the run
 * failed with that reason, after which the undos of its completed compensable steps are expected.
 */
public record ObserveRunContext(Instance inst, String reporter, List<StepInput> steps, boolean fin,
                                String failure, Tx tx) {
}
