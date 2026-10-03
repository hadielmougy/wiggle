package com.wiggle.client.worker;

import com.wiggle.client.WiggleClient.WiggleApiException;

import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Holds settle RPCs (step reports, failures) that could not reach the server before their lease ran
 * out, and resends them in order every {@code periodMillis} until the server answers. In memory only:
 * a worker that dies loses them, and the tasks re-run as after any crash.
 *
 * <p>A resend the server accepts is done. One it refuses -- typically because the task was reclaimed
 * once its lease lapsed -- is dropped, and the task re-runs elsewhere. A server that is still
 * unreachable ends the round; the rest wait for the next.
 */
final class Outbox implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(Outbox.class.getName());

    private record Parked(String taskId, Runnable settle) {}

    private final ConcurrentLinkedQueue<Parked> parked = new ConcurrentLinkedQueue<>();
    private final ScheduledExecutorService resender;

    Outbox(String workerId, long periodMillis) {
        this.resender = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "wiggle-outbox-" + workerId);
            t.setDaemon(true);
            return t;
        });
        long period = Math.max(1, periodMillis);
        resender.scheduleWithFixedDelay(this::resend, period, period, TimeUnit.MILLISECONDS);
    }

    /**
     * Delivers {@code settle} through {@link Heartbeat#deliver}. If the lease runs out with the server
     * still unreachable, parks {@code whenParked} -- the settle to send once it returns -- and answers
     * null.
     */
    <T> T deliverOrPark(Heartbeat lease, String taskId, Supplier<T> settle, Runnable whenParked) {
        try {
            return lease.deliver(settle);
        } catch (WiggleApiException e) {
            if (!Heartbeat.unreachable(e)) throw e;
            parked.add(new Parked(taskId, whenParked));
            LOG.log(System.Logger.Level.WARNING, () -> "lease of task " + taskId
                    + " ran out with the server unreachable; holding its result to resend");
            return null;
        }
    }

    void deliverOrPark(Heartbeat lease, String taskId, Runnable settle) {
        deliverOrPark(lease, taskId, () -> {
            settle.run();
            return null;
        }, settle);
    }

    int size() {
        return parked.size();
    }

    synchronized void resend() {
        Iterator<Parked> it = parked.iterator();
        while (it.hasNext()) {
            Parked p = it.next();
            try {
                p.settle().run();
                it.remove();
                LOG.log(System.Logger.Level.INFO, () -> "delivered held result of task " + p.taskId());
            } catch (WiggleApiException e) {
                if (Heartbeat.unreachable(e)) return;
                it.remove();
                LOG.log(System.Logger.Level.WARNING, () -> "server refused held result of task " + p.taskId()
                        + "; it will re-run: " + e.getMessage());
            } catch (RuntimeException e) {
                it.remove();
                LOG.log(System.Logger.Level.WARNING, () -> "dropped held result of task " + p.taskId() + ": " + e);
            }
        }
    }

    /** Stops resending after one last round; whatever is still held is lost. */
    @Override
    public void close() {
        resender.shutdownNow();
        resend();
        if (!parked.isEmpty()) {
            LOG.log(System.Logger.Level.WARNING, () -> "worker closing with " + parked.size()
                    + " undelivered result(s); those tasks will re-run");
            parked.clear();
        }
    }
}
