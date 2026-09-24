package com.wiggle.server.engine;

import com.wiggle.server.engine.WorkflowEngine.StepInput;

/**
 * One step's outcome as its reporter split it: a task's complete next context, or a predicate's
 * branch. Each node kind reads only the half it needs, so extraction stays lazy -- asking a task's
 * result for a branch is an error, and nothing asks unless the kind has branches.
 */
record StepReport(StepInput step) {

    static StepReport of(StepInput step) {
        return new StepReport(step);
    }

    /** The complete next context for a TASK node; null leaves the context untouched. */
    Object nextContext() {
        return step.merge();
    }

    /** The branch a PREDICATE node takes. */
    boolean predicate() {
        if (step.predicateValue() == null) {
            throw EngineException.badRequest("predicate result must be a boolean");
        }
        return step.predicateValue();
    }
}
