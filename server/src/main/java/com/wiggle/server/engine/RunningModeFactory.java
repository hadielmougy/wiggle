package com.wiggle.server.engine;

import com.wiggle.core.ExecutionMode;

import static com.wiggle.server.engine.RunningMode.resolveMode;

abstract class RunningModeFactory {

    final RunningMode create(ExecutionMode mode) {
        return switch (resolveMode(mode)) {
            case SERVER         -> new ServerRunningMode(instances(), nodeBehaviourFactory(), definitions());
            case LOCAL_SYNC     -> new LocalSyncRunningMode();
            case LOCAL_ASYNC    -> new LocalAsyncRunningMode();
            default -> throw new IllegalArgumentException("Unknown running mode: " + mode);
        };
    }

    abstract Instances instances();
    abstract NodeBehaviourFactory nodeBehaviourFactory();
    abstract DefinitionRegistry definitions();
}
