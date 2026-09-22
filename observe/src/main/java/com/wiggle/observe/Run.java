package com.wiggle.observe;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * One execution of an observed flow, from its first step to END. Steps land in a buffer that is
 * flushed when it fills, when it has lingered, or when the run ends; the first flush mints the
 * instance on the server and every later one names it. Closing a run that never reached END is
 * reported as such, which the server records as an incomplete run.
 *
 * <p>Opened explicitly with {@link Observed#begin} or implicitly by the first step called on a
 * thread with no run open. Steps are recorded on the calling thread; the buffer is shared with
 * the flusher, so both go through the lock.
 */
public final class Run implements AutoCloseable {

    private final Observed<?> owner;
    private final String correlationId;
    private final List<StepRecord> buffer = new ArrayList<>();
    private volatile String instanceId;
    private volatile String lastStatus;
    private long firstBufferedAt;
    private boolean reachedEnd;
    private boolean ended;
    /** The server answered with a terminal status, or a report was lost: nothing more is sent. */
    private volatile boolean dead;

    Run(Observed<?> owner, String correlationId) {
        this.owner = owner;
        this.correlationId = correlationId;
    }

    /** The server's id for this run once its first report has landed; null before that. */
    public String instanceId() {
        return instanceId;
    }

    /** The instance's status as of the last report that landed; null before the first. */
    public String lastStatus() {
        return lastStatus;
    }

    public String correlationId() {
        return correlationId;
    }

    /** Ends the run. Reaching END already ended it, in which case this is a no-op. */
    @Override
    public void close() {
        end();
    }

    void record(StepRecord step, boolean atEnd) {
        Batch flush = null;
        synchronized (this) {
            if (ended || dead) return;
            if (buffer.isEmpty()) firstBufferedAt = System.currentTimeMillis();
            buffer.add(step);
            reachedEnd = atEnd;
            if (atEnd) {
                ended = true;
                flush = take(true);
            } else if (buffer.size() >= owner.options().batchSize()) {
                flush = take(false);
            }
        }
        if (flush != null) owner.reporter().submit(flush);
        else owner.reporter().pending(this);
        if (atEnd) owner.detach(this);
    }

    void end() {
        Batch flush = null;
        synchronized (this) {
            if (!ended) {
                ended = true;
                if (!dead && (!buffer.isEmpty() || !reachedEnd)) flush = take(true);
            }
        }
        if (flush != null) owner.reporter().submit(flush);
        owner.detach(this);
    }

    /**
     * What the flusher takes from a run whose buffer has waited longer than the linger. A run
     * with nothing buffered leaves {@code pending} here, under its own lock, so a step recorded
     * meanwhile re-registers it rather than being missed.
     */
    Batch takeIfStale(long lingerMillis, long now, Set<Run> pending) {
        synchronized (this) {
            if (buffer.isEmpty()) {
                synchronized (pending) { pending.remove(this); }
                return null;
            }
            if (now - firstBufferedAt < lingerMillis) return null;
            return take(false);
        }
    }

    private Batch take(boolean fin) {
        Batch b = new Batch(this, List.copyOf(buffer), fin);
        buffer.clear();
        return b;
    }

    void landed(String instanceId, String status) {
        this.instanceId = instanceId;
        this.lastStatus = status;
        if (!"RUNNING".equals(status)) dead = true;
    }

    void lost() {
        dead = true;
    }

    boolean dead() {
        return dead;
    }

    Observed<?> owner() {
        return owner;
    }

    /** A flushed slice of a run: its steps in order, and whether the run ends with them. */
    record Batch(Run run, List<StepRecord> steps, boolean fin) {}
}
