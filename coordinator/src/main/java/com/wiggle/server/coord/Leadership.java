package com.wiggle.server.coord;

/**
 * Whether this coordinator process currently holds leadership. Reconciliation is a leader-only
 * duty, so it is asked afresh on every pass rather than latched -- leadership can be lost between
 * two ticks, and a former leader must stop acting at once.
 */
@FunctionalInterface
public interface Leadership {

    /** True while this process is the elected leader. */
    boolean held();
}
