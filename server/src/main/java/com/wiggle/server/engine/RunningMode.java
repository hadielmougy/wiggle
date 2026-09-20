package com.wiggle.server.engine;

import com.wiggle.core.ExecutionMode;

public interface RunningMode {

    void advance(RunningModeContext context);

    static ExecutionMode resolveMode(ExecutionMode mode) {
        return mode == null || mode == ExecutionMode.DEFAULT ? ExecutionMode.SERVER : mode;
    }
}
