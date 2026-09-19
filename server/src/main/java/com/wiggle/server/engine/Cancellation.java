package com.wiggle.server.engine;

/**
 * Whether the caller that asked for work has gone away -- a worker whose long poll was cancelled
 * because it is shutting down or its connection dropped. The engine asks before claiming, so a
 * freshly produced token goes to a live worker instead of being stranded until its lease expires.
 *
 * <p>Polled from the long poll's wait loop, so it must be cheap and non-blocking.
 */
@FunctionalInterface
public interface Cancellation {

    /** True once the request this poll serves is gone. */
    boolean cancelled();

    /** A caller that never goes away -- the short-poll path, and tests. */
    static Cancellation never() {
        return () -> false;
    }
}
