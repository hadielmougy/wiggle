package com.wiggle.observe;

/**
 * A step in progress, opened by {@link ObservedFlow#start}: closing it with one of the outcomes
 * records the step with the time between. A timer never closed records nothing.
 */
public final class StepTimer {

    private final ObservedFlow flow;
    private final String key;
    private final String step;
    private final long startedAt = System.currentTimeMillis();
    private final long t0 = System.nanoTime();
    private boolean closed;

    StepTimer(ObservedFlow flow, String key, String step) {
        this.flow = flow;
        this.key = key;
        this.step = step;
    }

    /** The step completed. */
    public void done() {
        if (close()) flow.record(key, step, startedAt, finishedAt());
    }

    /** The predicate completed with {@code value}. */
    public void decided(boolean value) {
        if (close()) flow.recordPredicate(key, step, value, startedAt, finishedAt());
    }

    /** The step threw. */
    public void failed(Throwable t) {
        String msg = t.getMessage();
        failed(t.getClass().getSimpleName() + (msg == null ? "" : ": " + msg));
    }

    public void failed(String error) {
        if (close()) flow.recordError(key, step, error, startedAt, finishedAt());
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
