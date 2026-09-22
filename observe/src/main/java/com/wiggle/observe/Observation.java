package com.wiggle.observe;

/**
 * The run the current thread's step calls belong to, readable from inside an observed step the
 * way a worker reads {@code Step}: a step that needs its run's key, to stamp on an outbound
 * message say, takes it from here rather than through its own signature.
 *
 * <p>Only valid on a thread with a run open -- inside an observed call, between
 * {@link Observed#begin} or {@link Observed#join} and the run's close, or inside a task the run
 * {@link Run#wrap wrapped}. Elsewhere these throw.
 */
public final class Observation {

    private static final ThreadLocal<Run> CURRENT = new ThreadLocal<>();

    private Observation() {}

    /** The current thread's run. */
    public static Run run() {
        Run r = CURRENT.get();
        if (r == null) {
            throw new IllegalStateException("no observed run is open on this thread: call it inside an observed "
                    + "step, between begin()/join() and close(), or in a task the run wrapped");
        }
        return r;
    }

    /** The current run's key. */
    public static String correlationId() {
        return run().correlationId();
    }

    /** The current run's instance id, once a report has landed; null before that. */
    public static String instanceId() {
        return run().instanceId();
    }

    /** The current run's context, to send along with an outbound message. */
    public static RunContext context() {
        return run().context();
    }

    static Run current() {
        return CURRENT.get();
    }

    /** Binds {@code run} to this thread and returns what was bound before, for {@link #restore}. */
    static Run attach(Run run) {
        Run before = CURRENT.get();
        CURRENT.set(run);
        return before;
    }

    static void restore(Run before) {
        if (before == null) CURRENT.remove();
        else CURRENT.set(before);
    }

    static void detach(Run run) {
        if (CURRENT.get() == run) CURRENT.remove();
    }
}
