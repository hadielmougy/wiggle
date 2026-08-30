package com.wiggle.client.worker;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Keeps one task's lease alive while its handler runs, and -- the reason this is its own
 * class -- guarantees that once the task is settled no further lease extension is ever sent.
 *
 * <p>The periodic beat and {@link #stop()} share a lock and a {@code settled} flag: a beat
 * that has not started yet sees {@code settled} and skips; a beat already in flight finishes
 * before {@code stop()} returns. So a caller that calls {@code stop()} <em>before</em>
 * completing or failing the task can be sure no extension races or trails the settle RPC.
 */
final class Heartbeat {

    /** Sends a lease extension of {@code extendMillis} for the task. */
    interface Sender {
        void extend(long extendMillis);
    }

    private static final System.Logger LOG = System.getLogger(Heartbeat.class.getName());

    private final ScheduledExecutorService scheduler;
    private final Sender sender;
    private final String taskId;
    private final long leaseMillis;
    private final long periodMillis;

    private final ReentrantLock lock = new ReentrantLock();
    private boolean settled;
    private ScheduledFuture<?> future;

    Heartbeat(ScheduledExecutorService scheduler, Sender sender, long leaseMillis, String taskId) {
        this.scheduler = scheduler;
        this.sender = sender;
        this.taskId = taskId;
        this.leaseMillis = leaseMillis;
        // Beat at a third of the lease so a single dropped beat is not fatal.
        this.periodMillis = Math.max(1, leaseMillis / 3);
    }

    void start() {
        future = scheduler.scheduleWithFixedDelay(this::beat, periodMillis, periodMillis, TimeUnit.MILLISECONDS);
    }

    private void beat() {
        lock.lock();
        try {
            if (settled) return;            // task already settled: never extend a finished lease
            sender.extend(leaseMillis);
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.DEBUG,
                    () -> "heartbeat for task " + taskId + " failed: " + e.getMessage());
        } finally {
            lock.unlock();
        }
    }

    /**
     * Marks the task settled and stops the beat. After this returns, no extension will be
     * sent for this task, so callers must invoke it before completing or failing the task.
     * Idempotent.
     */
    void stop() {
        lock.lock();
        try {
            settled = true;
        } finally {
            lock.unlock();
        }
        if (future != null) future.cancel(false);
    }
}
