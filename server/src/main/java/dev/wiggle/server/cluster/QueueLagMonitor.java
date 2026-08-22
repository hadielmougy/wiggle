package dev.wiggle.server.cluster;

import dev.wiggle.server.engine.WorkflowEngine;
import dev.wiggle.server.store.Rows;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Leader-only background thread that watches whether the dispatchable backlog is being
 * drained fast enough, and logs a WARNING when it isn't.
 *
 * <p>Runs independently of {@link Housekeeper} (its own thread, its own cadence) because it
 * measures a different thing: not "is there work to advance" but "is the system keeping up
 * with demand." The backlog size and the consumption rate are both read from the shared
 * database rather than kept in memory, so the numbers reflect every node's throughput, not
 * just this one's -- a non-leader node claiming work still counts.
 */
public final class QueueLagMonitor implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(QueueLagMonitor.class.getName());

    private final WorkflowEngine engine;
    private final ClusterManager cluster;
    private final Duration checkInterval;
    private final long warnThresholdMillis;
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "wiggle-queue-lag");
                t.setDaemon(true);
                return t;
            });

    /** 0 means no baseline yet -- the first tick (or the first after regaining leadership) only primes it. */
    private volatile long lastCheckAt;

    public QueueLagMonitor(WorkflowEngine engine, ClusterManager cluster,
                           Duration checkInterval, Duration warnThreshold) {
        this.engine = engine;
        this.cluster = cluster;
        this.checkInterval = checkInterval;
        this.warnThresholdMillis = warnThreshold.toMillis();
    }

    public void start() {
        long period = Math.max(200, checkInterval.toMillis());
        scheduler.scheduleAtFixedRate(this::tick, period, period, TimeUnit.MILLISECONDS);
    }

    private void tick() {
        try {
            check();
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "queue lag check failed: " + e);
        }
    }

    /** The measurement, split out from {@link #tick()} so it can be driven directly in tests. */
    void check() {
        if (!cluster.isLeader()) {
            // Don't let a stale baseline from a previous leadership stint produce a bogus rate
            // once we re-acquire it.
            lastCheckAt = 0;
            return;
        }

        long now = System.currentTimeMillis();
        Rows.QueueDepth depth = engine.queueDepth();

        if (lastCheckAt == 0) {
            lastCheckAt = now;
            LOG.log(System.Logger.Level.DEBUG, () ->
                    "queue lag monitor: baseline established, backlog=" + depth.readyCount());
            return;
        }

        long elapsedMillis = now - lastCheckAt;
        if (elapsedMillis <= 0) return;   // clock hasn't advanced since the last check; nothing to measure
        int processed = engine.tasksProcessedSince(lastCheckAt);
        double rateTasksPerSec = processed * 1000.0 / elapsedMillis;
        lastCheckAt = now;

        int backlog = depth.readyCount();
        if (backlog == 0) {
            LOG.log(System.Logger.Level.DEBUG, () -> String.format(
                    "queue lag monitor: backlog empty, consumption rate=%.2f tasks/sec", rateTasksPerSec));
            return;
        }

        long oldestWaitMillis = depth.oldestAvailableAt() > 0 ? now - depth.oldestAvailableAt() : 0;
        double lagSeconds = rateTasksPerSec > 0 ? backlog / rateTasksPerSec : Double.POSITIVE_INFINITY;
        boolean laggingByRate = lagSeconds * 1000 > warnThresholdMillis;
        boolean staleOldest = oldestWaitMillis > warnThresholdMillis;

        if (laggingByRate || staleOldest) {
            LOG.log(System.Logger.Level.WARNING, () -> String.format(
                    "queue lag: %d task(s) queued, consumption rate=%.2f tasks/sec, "
                            + "estimated drain time=%s, oldest queued task has waited %dms",
                    backlog, rateTasksPerSec,
                    Double.isInfinite(lagSeconds) ? "never (no throughput)" : String.format("%.1fs", lagSeconds),
                    oldestWaitMillis));
        } else {
            LOG.log(System.Logger.Level.DEBUG, () -> String.format(
                    "queue lag monitor: backlog=%d, consumption rate=%.2f tasks/sec, draining within budget",
                    backlog, rateTasksPerSec));
        }
    }

    @Override public void close() {
        scheduler.shutdownNow();
    }
}
