package com.wiggle.observe;

/** One completed step of a run as this side recorded it; {@code undoOf} names the compensable
 *  step this one reversed, when it is an undo. */
record StepRecord(String nodeId, Object merge, Boolean predicateValue, String error,
                  long startedAt, long finishedAt, String afterNode, String undoOf) {

    static StepRecord step(String nodeId, Object merge, long startedAt, long finishedAt, String after) {
        return new StepRecord(nodeId, merge, null, null, startedAt, finishedAt, after, null);
    }

    static StepRecord predicate(String nodeId, boolean value, long startedAt, long finishedAt, String after) {
        return new StepRecord(nodeId, null, value, null, startedAt, finishedAt, after, null);
    }

    static StepRecord error(String nodeId, String error, long startedAt, long finishedAt, String after) {
        return new StepRecord(nodeId, null, null, error, startedAt, finishedAt, after, null);
    }

    static StepRecord undo(String nodeId, String error, long startedAt, long finishedAt, String after) {
        return new StepRecord(nodeId, null, null, error, startedAt, finishedAt, after, nodeId);
    }
}
