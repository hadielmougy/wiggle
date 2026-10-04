package com.wiggle.server.cluster;

import com.wiggle.server.engine.WorkflowEngine;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Leader-only background duties. Non-leaders keep serving the HTTP API and handing
 * out work; only the leader touches the clock-driven parts of the system, which keeps
 * timer firing and orphan reclamation from being done N times over in an N-node cluster.
 *
 * <p>For {@code reconnectGrace} after this node becomes leader, a task claimed before this node
 * started is not reclaimed even if its lease has run out: a lease that lapsed while the cell was down
 * is no evidence its worker died, and the worker gets that long to reconnect and report or heartbeat.
 */
public final class Housekeeper implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(Housekeeper.class.getName());

    private final WorkflowEngine engine;
    private final ClusterManager cluster;
    private final Duration pollInterval;
    private final Duration retention;
    private final int batchSize;
    private final boolean adaptive;
    private final Duration reconnectGrace;
    private final long createdAt = System.currentTimeMillis();
    private long leaderSince = -1;
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "wiggle-housekeeper");
                t.setDaemon(true);
                return t;
            });

    public Housekeeper(WorkflowEngine engine, ClusterManager cluster, Duration pollInterval,
                       Duration retention, int batchSize) {
        this(engine, cluster, pollInterval, retention, batchSize, adaptiveByDefault());
    }

    /** {@code wiggle.adaptive.housekeeping} / {@code WIGGLE_ADAPTIVE_HOUSEKEEPING}, default true. */
    public static boolean adaptiveByDefault() {
        return Boolean.parseBoolean(System.getProperty("wiggle.adaptive.housekeeping",
                System.getenv().getOrDefault("WIGGLE_ADAPTIVE_HOUSEKEEPING", "true")));
    }

    /**
     * @param adaptive when true, a sweep that fills its batch runs again immediately (drain mode)
     *                 instead of leaving the excess for the next tick. The signal is batch fullness
     *                 -- every extra sweep is one that just proved it had work -- so the loop cannot
     *                 spin idle, and the steady-state cost when nothing is due is unchanged. Without
     *                 it, due work is promoted at most {@code batchSize} per tick, which caps timer /
     *                 schedule / reclaim throughput at batch÷interval (~100/s on defaults).
     */
    public Housekeeper(WorkflowEngine engine, ClusterManager cluster, Duration pollInterval,
                       Duration retention, int batchSize, boolean adaptive) {
        this(engine, cluster, pollInterval, retention, batchSize, adaptive, Duration.ZERO);
    }

    public Housekeeper(WorkflowEngine engine, ClusterManager cluster, Duration pollInterval,
                       Duration retention, int batchSize, boolean adaptive, Duration reconnectGrace) {
        this.engine = engine;
        this.cluster = cluster;
        this.pollInterval = pollInterval;
        this.retention = retention;
        this.batchSize = batchSize;
        this.adaptive = adaptive;
        this.reconnectGrace = reconnectGrace;
    }

    public void start() {
        long period = Math.max(200, pollInterval.toMillis());
        scheduler.scheduleAtFixedRate(this::tick, period, period, TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(this::retain, retention.toMillis() / 4 + 1000,
                Math.max(60_000, retention.toMillis() / 4), TimeUnit.MILLISECONDS);
    }

    /** Package-visible so tests can drive a tick deterministically. */
    void tick() {
        if (!cluster.isLeader()) {
            leaderSince = -1;
            LOG.log(System.Logger.Level.DEBUG, "housekeeping tick: skipped, not leader");
            return;
        }
        long now = System.currentTimeMillis();
        if (leaderSince < 0) leaderSince = now;
        long spareClaimedBefore = now < leaderSince + reconnectGrace.toMillis() ? createdAt : Long.MIN_VALUE;
        try {
            LOG.log(System.Logger.Level.DEBUG, "housekeeping tick: leader running timers/leases/deadlines sweep");
            int fired = 0, reclaimed = 0, escalated = 0, scheduled = 0, retried = 0, rounds = 0;
            boolean anyFull;
            do {
                int f = engine.fireDueTimers(batchSize);
                int p = engine.promoteDueRetries(batchSize);
                int r = engine.reclaimExpiredLeases(batchSize, spareClaimedBefore);
                int e = engine.fireDueSignalDeadlines(batchSize);
                int s = engine.fireDueSchedules(batchSize);
                fired += f; retried += p; reclaimed += r; escalated += e; scheduled += s; rounds++;
                // Drain mode: a full batch means more work is (almost certainly) still due -- go
                // again now rather than parking it for a whole tick. Bounded by real work: every
                // extra round fired a full batch, so an idle system never loops.
                anyFull = adaptive && (f >= batchSize || p >= batchSize || r >= batchSize || e >= batchSize
                        || s >= batchSize);
            } while (anyFull && cluster.isLeader() && !Thread.currentThread().isInterrupted());
            int fFired = fired, fRetried = retried, fReclaimed = reclaimed, fEscalated = escalated,
                    fScheduled = scheduled, fRounds = rounds;
            if (rounds > 1) LOG.log(System.Logger.Level.INFO, () -> "housekeeping drain: " + fRounds
                    + " rounds in one tick (" + fFired + " timers, " + fRetried + " retries, " + fReclaimed
                    + " leases, " + fEscalated + " deadlines, " + fScheduled + " schedules)");
            else LOG.log(System.Logger.Level.DEBUG, () -> "housekeeping tick: " + fFired + " timers fired, "
                    + fRetried + " retries promoted, " + fReclaimed + " leases reclaimed, " + fEscalated
                    + " signal deadlines fired, " + fScheduled + " schedules fired");
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "housekeeping tick failed: " + e);
        }
    }

    /** Package-visible so tests can drive a sweep deterministically. */
    void retain() {
        if (!cluster.isLeader()) {
            LOG.log(System.Logger.Level.DEBUG, "retention sweep: skipped, not leader");
            return;
        }
        try {
            LOG.log(System.Logger.Level.DEBUG, "retention sweep: leader running");
            int purged = engine.purgeTerminalInstancesOlderThan(retention.toMillis(), batchSize * 10);
            int trimmed = engine.trimEvents(batchSize * 10);
            if (purged > 0 || trimmed > 0) LOG.log(System.Logger.Level.INFO, () -> "purged " + purged
                    + " terminal instances, trimmed " + trimmed + " events");
            else LOG.log(System.Logger.Level.DEBUG, "retention sweep: nothing to purge");
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "retention sweep failed: " + e);
        }
    }

    @Override public void close() {
        scheduler.shutdownNow();
    }
}
