package com.wiggle.client.worker;

import com.wiggle.client.WiggleClient.WiggleApiException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * Keeps one task's lease alive while its handler runs, and guarantees no lease extension is sent
 * once the task is settled.
 *
 * <p>The periodic beat and {@link #stop()} share a lock and a {@code settled} flag: a beat that has
 * not started sees {@code settled} and skips; one already in flight finishes before {@code stop()}
 * returns. A caller that stops before completing or failing the task gets no extension trailing the
 * settle RPC.
 *
 * <p>It also tracks how long the lease is known to last: from construction, then from the send of
 * each acknowledged extension (or a {@link #renewed()} continuation). {@link #deliver} retries a
 * settle RPC the server could not be reached for until that deadline passes.
 */
final class Heartbeat {

    /** Sends a lease extension of {@code extendMillis} for the task. */
    interface Sender {
        void extend(long extendMillis);
    }

    private static final System.Logger LOG = System.getLogger(Heartbeat.class.getName());
    private static final long DELIVERY_PAUSE_MILLIS = 500;

    private final ScheduledExecutorService scheduler;
    private final Sender sender;
    private final String taskId;
    private final long leaseMillis;
    private final long periodMillis;

    private final ReentrantLock lock = new ReentrantLock();
    private boolean settled;
    private ScheduledFuture<?> future;
    private volatile long deadlineNanos;

    Heartbeat(ScheduledExecutorService scheduler, Sender sender, long leaseMillis, String taskId) {
        this.scheduler = scheduler;
        this.sender = sender;
        this.taskId = taskId;
        this.leaseMillis = leaseMillis;
        // Beat at a third of the lease so a single dropped beat is not fatal.
        this.periodMillis = Math.max(1, leaseMillis / 3);
        renewed();
    }

    void start() {
        future = scheduler.scheduleWithFixedDelay(this::beat, periodMillis, periodMillis, TimeUnit.MILLISECONDS);
    }

    private void beat() {
        lock.lock();
        try {
            if (settled) return;            // task already settled: never extend a finished lease
            long sentAt = System.nanoTime();
            sender.extend(leaseMillis);
            deadlineNanos = sentAt + TimeUnit.MILLISECONDS.toNanos(leaseMillis);
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

    /** Restarts the deadline from now: the server just granted a fresh lease (a leased continuation). */
    void renewed() {
        deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(leaseMillis);
    }

    /**
     * Runs a settle RPC, repeating it while it fails because the server is unreachable and the lease
     * has not yet run out. Any other failure, an unreachable server past the deadline, or an
     * interrupt is thrown as-is.
     */
    <T> T deliver(Supplier<T> settle) {
        while (true) {
            try {
                return settle.get();
            } catch (WiggleApiException e) {
                long remainingMillis = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
                if (!unreachable(e) || remainingMillis <= 0 || Thread.currentThread().isInterrupted()) throw e;
                LOG.log(System.Logger.Level.WARNING, () -> "server unreachable settling task " + taskId
                        + "; retrying for up to " + remainingMillis + "ms: " + e.getMessage());
                Worker.sleep(Math.min(DELIVERY_PAUSE_MILLIS, remainingMillis));
            }
        }
    }

    void deliver(Runnable settle) {
        deliver(() -> {
            settle.run();
            return null;
        });
    }

    private static boolean unreachable(WiggleApiException e) {
        return e.getCause() instanceof StatusRuntimeException s && s.getStatus().getCode() == Status.Code.UNAVAILABLE;
    }
}
