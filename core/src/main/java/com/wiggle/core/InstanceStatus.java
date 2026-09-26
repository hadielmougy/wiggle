package com.wiggle.core;

/**
 * The status an instance is persisted with, and the one authority on what each status means for
 * the instance's life. Shared by the engine, the stores and the wire types, so the same
 * classification answers every reader rather than each spelling its own.
 */
public enum InstanceStatus {

    RUNNING(Phase.FORWARD),
    COMPENSATING(Phase.REVERSE),
    COMPLETED(Phase.FINISHED),
    FAILED(Phase.FINISHED),
    CANCELLED(Phase.FINISHED),
    COMPENSATED(Phase.FINISHED),
    COMPENSATION_FAILED(Phase.FINISHED);

    private enum Phase { FORWARD, REVERSE, FINISHED }

    private final Phase phase;

    InstanceStatus(Phase phase) {
        this.phase = phase;
    }

    /** Not finished: either running forward, or undoing. The complement of terminal. */
    public boolean live() {
        return phase != Phase.FINISHED;
    }

    /** The forward flow is live: work may be reported against it, and a finished sub-workflow may
     *  resume its parent's token. Only RUNNING -- COMPENSATING is going backwards. */
    public boolean running() {
        return phase == Phase.FORWARD;
    }

    /** The reverse pass owns the instance: an undo may settle its comp-log entry here, and only here. */
    public boolean compensating() {
        return phase == Phase.REVERSE;
    }

    /** Whether the status named by {@code name} is live, for a reader holding the wire string
     *  rather than the enum. An unrecognised name is not live: a status this build cannot name is
     *  never treated as still running. */
    public static boolean liveByName(String name) {
        for (InstanceStatus s : values()) {
            if (s.name().equals(name)) return s.live();
        }
        return false;
    }

    /** Whether the status named by {@code name} is the live forward flow. Unrecognised is not. */
    public static boolean runningByName(String name) {
        for (InstanceStatus s : values()) {
            if (s.name().equals(name)) return s.running();
        }
        return false;
    }
}
