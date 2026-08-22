package dev.wiggle.server.cluster;

import dev.wiggle.server.engine.WorkflowEngine;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Leader-only background duties. Non-leaders keep serving the HTTP API and handing
 * out work; only the leader touches the clock-driven parts of the system, which keeps
 * timer firing and orphan reclamation from being done N times over in an N-node cluster.
 */
public final class Housekeeper implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(Housekeeper.class.getName());

    private final WorkflowEngine engine;
    private final ClusterManager cluster;
    private final Duration pollInterval;
    private final Duration retention;
    private final int batchSize;
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "wiggle-housekeeper");
                t.setDaemon(true);
                return t;
            });

    public Housekeeper(WorkflowEngine engine, ClusterManager cluster, Duration pollInterval,
                       Duration retention, int batchSize) {
        this.engine = engine;
        this.cluster = cluster;
        this.pollInterval = pollInterval;
        this.retention = retention;
        this.batchSize = batchSize;
    }

    public void start() {
        long period = Math.max(200, pollInterval.toMillis());
        scheduler.scheduleAtFixedRate(this::tick, period, period, TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(this::retain, retention.toMillis() / 4 + 1000,
                Math.max(60_000, retention.toMillis() / 4), TimeUnit.MILLISECONDS);
    }

    private void tick() {
        if (!cluster.isLeader()) return;
        try {
            int fired = engine.fireDueTimers(batchSize);
            int reclaimed = engine.reclaimExpiredLeases(batchSize);
            int escalated = engine.fireDueUserTaskDeadlines(batchSize);
            if (fired > 0 || reclaimed > 0 || escalated > 0) {
                LOG.log(System.Logger.Level.DEBUG, () -> "housekeeping: " + fired + " timers fired, "
                        + reclaimed + " leases reclaimed, " + escalated + " user-task deadlines fired");
            }
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "housekeeping tick failed: " + e);
        }
    }

    private void retain() {
        if (!cluster.isLeader()) return;
        try {
            int purged = engine.purgeTerminalInstancesOlderThan(retention.toMillis(), batchSize * 10);
            if (purged > 0) LOG.log(System.Logger.Level.INFO, () -> "purged " + purged + " terminal instances");
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "retention sweep failed: " + e);
        }
    }

    @Override public void close() {
        scheduler.shutdownNow();
    }
}
