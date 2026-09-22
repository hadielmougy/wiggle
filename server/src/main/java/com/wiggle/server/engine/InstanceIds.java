package com.wiggle.server.engine;

import com.wiggle.core.Ids;

/** Mints instance ids. {@link #forKey} derives one from a business key instead, so every
 *  reporter of one observed run lands on the same instance. */
@FunctionalInterface
public interface InstanceIds {

    String next();

    /** The id an observed run keyed by {@code key} has on this cell; the same key always yields it. */
    default String forKey(String key) {
        return "wfo_" + Ids.digest(key);
    }
}
