package com.wiggle.server.engine;

import com.wiggle.core.ExecutionMode;

/**
 * How a workflow's steps are executed and reported. Each mode owns its own procedure: the
 * engine locks the instance, resolves the mode, and hands off one {@link ExecutionContext} through
 * {@code execute} -- the single entry point for every operation. The context, not the caller,
 * decides which procedure runs ({@link ExecutionContext#runOn}); a mode that does not support a
 * given context's operation refuses it there.
 */
public interface RunningMode {

    <T> T execute(ExecutionContext<T> ctx);

    /** The mode a definition actually runs under; DEFAULT and an absent mode both mean SERVER. */
    static ExecutionMode resolveMode(ExecutionMode mode) {
        return mode == null || mode == ExecutionMode.DEFAULT ? ExecutionMode.SERVER : mode;
    }
}
