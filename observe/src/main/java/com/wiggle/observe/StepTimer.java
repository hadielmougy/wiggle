package com.wiggle.observe;

/**
 * A step in progress, opened by {@link ObservedFlow#start}: closing it with one of the outcomes
 * records the step with the time between. A timer never closed records nothing.
 */
public final class StepTimer {

    private final ObservedFlow flow;
    private final String key;
    private final String step;
    private final boolean undo;
    private final long startedAt = System.currentTimeMillis();
    private final long t0 = System.nanoTime();
    private boolean closed;

    StepTimer(ObservedFlow flow, String key, String step, boolean undo) {
        this.flow = flow;
        this.key = key;
        this.step = step;
        this.undo = undo;
    }

    /** The step completed. */
    public void done() {
        done(null);
    }

    /** The step completed, and {@code after} is the step that caused it. */
    public void done(String after) {
        if (close()) {
            if (undo) flow.recordUndo(key, step, startedAt, finishedAt(), after);
            else flow.record(key, step, startedAt, finishedAt(), after);
        }
    }

    /** The predicate completed with {@code value}. */
    public void decided(boolean value) {
        if (close()) flow.recordPredicate(key, step, value, startedAt, finishedAt(), null);
    }

    /** The step threw. */
    public void failed(Throwable t) {
        failed(Observed.describe(t));
    }

    public void failed(String error) {
        if (close()) {
            if (undo) flow.recordUndoError(key, step, error, startedAt, finishedAt(), null);
            else flow.recordError(key, step, error, startedAt, finishedAt(), null);
        }
    }

    private long finishedAt() {
        return startedAt + (System.nanoTime() - t0) / 1_000_000;
    }

    private boolean close() {
        if (closed) return false;
        closed = true;
        return true;
    }
}
