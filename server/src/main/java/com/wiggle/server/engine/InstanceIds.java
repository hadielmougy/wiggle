package com.wiggle.server.engine;

import com.wiggle.core.Ids;

/** Mints instance ids. {@link #forKey} derives one from a run's key instead, so every reporter
 *  of one observed run lands on the same instance. */
@FunctionalInterface
public interface InstanceIds {

    String next();

    /** The id the observed run of {@code workflow} keyed by {@code key} has; the same key always yields it. */
    default String forKey(String workflow, String key) {
        return "wfo_" + Ids.digest(workflow + ":" + key);
    }
}
