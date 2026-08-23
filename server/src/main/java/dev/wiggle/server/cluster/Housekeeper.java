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

    /** Package-visible so tests can drive a tick deterministically. */
    void tick() {
        if (!cluster.isLeader()) {
            LOG.log(System.Logger.Level.DEBUG, "housekeeping tick: skipped, not leader");
            return;
        }
        try {
            LOG.log(System.Logger.Level.DEBUG, "housekeeping tick: leader running timers/leases/deadlines sweep");
            int fired = engine.fireDueTimers(batchSize);
            int reclaimed = engine.reclaimExpiredLeases(batchSize);
            int escalated = engine.fireDueSignalDeadlines(batchSize);
            int scheduled = engine.fireDueSchedules(batchSize);
            LOG.log(System.Logger.Level.DEBUG, () -> "housekeeping tick: " + fired + " timers fired, "
                    + reclaimed + " leases reclaimed, " + escalated + " signal deadlines fired, "
                    + scheduled + " schedules fired");
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
            if (purged > 0) LOG.log(System.Logger.Level.INFO, () -> "purged " + purged + " terminal instances");
            else LOG.log(System.Logger.Level.DEBUG, "retention sweep: nothing to purge");
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "retention sweep failed: " + e);
        }
    }

    @Override public void close() {
        scheduler.shutdownNow();
    }
}
