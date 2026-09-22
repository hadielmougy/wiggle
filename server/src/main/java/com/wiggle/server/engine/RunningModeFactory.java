package com.wiggle.server.engine;

import com.wiggle.core.ExecutionMode;

import static com.wiggle.server.engine.RunningMode.resolveMode;

abstract class RunningModeFactory {

    final RunningMode create(ExecutionMode mode) {
        return switch (resolveMode(mode)) {
            case SERVER         -> new ServerRunningMode(instances(), nodeBehaviourFactory(), definitions());
            case LOCAL_SYNC     -> new LocalSyncRunningMode(instances(), nodeBehaviourFactory(), definitions());
            case LOCAL_ASYNC    -> localAsync();
            case OBSERVED       -> observed();
            default -> throw new IllegalArgumentException("Unknown running mode: " + mode);
        };
    }

    /** Typed: {@code advanceMany} exists on this mode alone, so its callers need the class. */
    final LocalAsyncRunningMode localAsync() {
        return new LocalAsyncRunningMode(instances(), nodeBehaviourFactory(), definitions());
    }

    /** Typed: {@code observe} exists on this mode alone. */
    final ObservedRunningMode observed() {
        return new ObservedRunningMode(instances(), nodeBehaviourFactory(), definitions());
    }

    abstract Instances instances();
    abstract NodeBehaviourFactory nodeBehaviourFactory();
    abstract DefinitionRegistry definitions();
}
