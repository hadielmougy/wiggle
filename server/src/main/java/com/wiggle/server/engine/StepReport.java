package com.wiggle.server.engine;

import com.wiggle.server.engine.WorkflowEngine.StepInput;

import java.util.Map;

/**
 * One step's outcome as a worker reported it. The two report paths carry it differently -- a
 * completion sends the handler's raw return, a reported run sends a pre-split {@link StepInput}
 * -- and each node kind reads only the half it needs, so extraction stays lazy: asking a task's
 * result for a branch is an error, and nothing asks unless the kind has branches.
 */
sealed interface StepReport {

    /** The complete next context for a TASK node; null leaves the context untouched. */
    Object nextContext();

    /** The branch a PREDICATE node takes. */
    boolean predicate();

    static StepReport of(Object result) {
        return new Completion(result);
    }

    static StepReport of(StepInput step) {
        return new Reported(step);
    }

    /** A worker's raw handler return: the branch has to be parsed back out of it. */
    record Completion(Object result) implements StepReport {

        @Override public Object nextContext() {
            return result;
        }

        @Override public boolean predicate() {
            if (result instanceof Boolean b) return b;
            if (result instanceof Map<?, ?> m && m.get("value") instanceof Boolean b) return b;
            throw EngineException.badRequest("predicate result must be a boolean or {\"value\": <boolean>}");
        }
    }

    /** A step of a locally-executed run: the worker already split merge from branch. */
    record Reported(StepInput step) implements StepReport {

        @Override public Object nextContext() {
            return step.merge();
        }

        @Override public boolean predicate() {
            return step.predicateValue() != null && step.predicateValue();
        }
    }
}
