package com.wiggle.observe;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * This process's part in one execution of an observed flow, under the run's key. Steps land in a
 * buffer that is flushed when it fills, when it has lingered, when a step's successor is END, or
 * when the run closes; every flush names the key, so the server maps it to the one instance the
 * key has whichever service reports first.
 *
 * <p>Three ways to hold one. An <em>implicit</em> run is opened by the first step called on a
 * thread with none open and ends when a step's successor is END: the thread's next step starts a
 * fresh one. A <em>begun</em> run ({@link Observed#begin}) is the originator's: it lives until it
 * is closed, and closing it before END tells the server the run is over. A <em>joined</em> run
 * ({@link Observed#join}) is a participant's: it lives until closed too, but closing it says
 * nothing about the whole, since other services may still be reporting.
 *
 * <p>A run belongs to the thread that opened it; {@link #wrap} carries it to another.
 */
public final class Run implements AutoCloseable {

    /** {@code API}: one explicit report through {@link ObservedFlow}, bound to no thread. */
    enum Kind { IMPLICIT, BEGUN, JOINED, API }

    private final ObservedFlow owner;
    private final Kind kind;
    private final String correlationId;
    private final List<StepRecord> buffer = new ArrayList<>();
    private volatile String instanceId;
    private volatile String lastStatus;
    /** The last step this run completed, as this side saw it: the next step's cause, and what a
     *  message sent now carries. Seeded from the sender's context when the run was joined. */
    private volatile String lastCompleted;
    private long firstBufferedAt;
    private boolean reachedEnd;
    private boolean ended;
    /** The server answered with a terminal status, or a report was lost: nothing more is sent. */
    private volatile boolean dead;

    Run(ObservedFlow owner, Kind kind, String correlationId) {
        this(owner, kind, correlationId, null);
    }

    Run(ObservedFlow owner, Kind kind, String correlationId, String after) {
        this.owner = owner;
        this.kind = kind;
        this.correlationId = correlationId;
        this.lastCompleted = after;
    }

    /** The server's id for the run once a report has landed; null before that. */
    public String instanceId() {
        return instanceId;
    }

    /** The instance's status as of the last report that landed; null before the first. */
    public String lastStatus() {
        return lastStatus;
    }

    /** The run's key: what every service reporting this run names. Never null. */
    public String correlationId() {
        return correlationId;
    }

    /**
     * What to send along with a message so the receiving service can {@link Observed#join} this
     * run: its identity, and the step completed last on this side, which the receiver's first step
     * will name as its cause.
     */
    public RunContext context() {
        return new RunContext(owner.name(), owner.version(), correlationId, lastCompleted);
    }

    /** The node of the step this run last completed on this side, or null. */
    public String lastCompleted() {
        return lastCompleted;
    }

    void completed(String nodeId) {
        lastCompleted = nodeId;
    }

    /** {@code task}, bound to this run while it executes on whatever thread runs it. */
    public Runnable wrap(Runnable task) {
        return () -> {
            Run before = Observation.attach(this);
            try { task.run(); } finally { Observation.restore(before); }
        };
    }

    /** {@link #wrap(Runnable)} for a task with a result. */
    public <V> Callable<V> wrap(Callable<V> task) {
        return () -> {
            Run before = Observation.attach(this);
            try { return task.call(); } finally { Observation.restore(before); }
        };
    }

    /**
     * Closes the run on this side. An originator closing before END reports the run as over;
     * a participant's close only flushes what it still holds.
     */
    @Override
    public void close() {
        end();
    }

    /**
     * Declares the run failed with a reason, from any side: the undos of its completed compensable
     * steps are now expected. What this side still holds goes with it, and the run ends here.
     */
    public void fail(String reason) {
        Batch flush;
        synchronized (this) {
            if (ended) return;
            ended = true;
            flush = new Batch(this, List.copyOf(buffer), false, reason == null || reason.isBlank() ? "failed" : reason);
            buffer.clear();
        }
        if (!dead) owner.reporter().submit(flush);
        Observation.detach(this);
    }

    void record(StepRecord step, boolean atEnd) {
        Batch flush = null;
        synchronized (this) {
            if (ended || dead) return;
            if (buffer.isEmpty()) firstBufferedAt = System.currentTimeMillis();
            buffer.add(step);
            if (atEnd) {
                reachedEnd = true;
                if (kind == Kind.IMPLICIT) ended = true;
                flush = take(kind != Kind.JOINED);
            } else if (buffer.size() >= owner.options().batchSize()) {
                flush = take(false);
            }
        }
        if (flush != null) owner.reporter().submit(flush);
        else owner.reporter().pending(this);
        if (atEnd && kind == Kind.IMPLICIT) Observation.detach(this);
    }

    /** A step of this run threw: the run is over on this side. */
    void failed() {
        end();
    }

    void end() {
        Batch flush = null;
        synchronized (this) {
            if (!ended) {
                ended = true;
                boolean fin = kind != Kind.JOINED;
                if (!dead && (!buffer.isEmpty() || (fin && !reachedEnd))) flush = take(fin);
            }
        }
        if (flush != null) owner.reporter().submit(flush);
        Observation.detach(this);
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
        Batch b = new Batch(this, List.copyOf(buffer), fin, null);
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

    ObservedFlow owner() {
        return owner;
    }

    /** A flushed slice of a run: its steps in order, whether the originator says the run ends with
     *  them, and a failure declared with them. */
    record Batch(Run run, List<StepRecord> steps, boolean fin, String failure) {}
}
