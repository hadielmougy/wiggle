package com.wiggle.server.engine;

/**
 * Where a newly dispatchable queue is reported, so the poller waiting on it can be woken once the
 * producing transaction commits (Layer 1; see docs/archive/in-memory-dispatch.md).
 *
 * <p>Marking is deliberately separate from signalling: a token parked READY is not claimable until
 * its transaction commits, so waking a poller any earlier would hand it work the database cannot
 * yet see. {@link Transactions} collects the marks and fires them post-commit.
 */
@FunctionalInterface
interface QueueWake {

    /** Marks {@code queue} as having gained work. A null queue is a no-op. */
    void ready(String queue);
}
