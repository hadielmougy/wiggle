package com.wiggle.server.engine;

import com.wiggle.core.ExecutionMode;

/** What the engine reads off a definition's persisted {@link ExecutionMode}. */
final class ExecutionModes {

    private ExecutionModes() {}

    /** The mode a definition actually runs under; DEFAULT and an absent mode both mean SERVER. */
    static ExecutionMode resolve(ExecutionMode mode) {
        return mode == null || mode == ExecutionMode.DEFAULT ? ExecutionMode.SERVER : mode;
    }

    /**
     * Whether a worker-reported run may lease its continuation straight back to the reporting
     * worker. SERVER hands back; the local modes chain. OBSERVED has no worker-reported runs.
     */
    static boolean chainsBack(ExecutionMode mode) {
        return switch (resolve(mode)) {
            case SERVER -> false;
            case LOCAL_SYNC, LOCAL_ASYNC -> true;
            default -> throw new IllegalArgumentException(mode + " has no worker-reported runs");
        };
    }
}
