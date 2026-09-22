package com.wiggle.tests;

import com.wiggle.client.flow.WiggleFlow;
import com.wiggle.core.ExecutionMode;

/** A mode sweep's way of choosing a definition's mode from a value, which the DSL itself does not offer. */
public final class Modes {

    private Modes() {}

    public static <T> WiggleFlow<T> in(WiggleFlow<T> f, ExecutionMode mode) {
        return switch (mode) {
            case SERVER      -> f.executeInServer();
            case LOCAL_SYNC  -> f.executeInLocalSync();
            case LOCAL_ASYNC -> f.executeInLocalAsync();
            case DEFAULT     -> f;
            case OBSERVED    -> throw new IllegalArgumentException("OBSERVED is stamped by an observer, not a spec");
        };
    }
}
