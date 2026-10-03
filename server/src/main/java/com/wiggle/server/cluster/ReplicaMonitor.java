package com.wiggle.server.cluster;

import com.wiggle.server.store.Storage;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Keeps read-replica lag measured: about once a second the leader stamps the heartbeat on every
 * primary, and this node probes every replica for how far that heartbeat trails its clock.
 */
public final class ReplicaMonitor implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(ReplicaMonitor.class.getName());

    private final Storage storage;
    private final ClusterManager cluster;
    private final long periodMillis;
    private ScheduledExecutorService scheduler;

    public ReplicaMonitor(Storage storage, ClusterManager cluster, long periodMillis) {
        this.storage = storage;
        this.cluster = cluster;
        this.periodMillis = periodMillis;
    }

    /** Starts measuring; does nothing on a store without replicas. */
    public synchronized void start() {
        if (scheduler != null || !storage.hasReplicas()) return;
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "wiggle-replica-monitor");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(this::tick, 0, periodMillis, TimeUnit.MILLISECONDS);
    }

    /** One round: the leader's heartbeat, then this node's probes. */
    void tick() {
        try {
            if (cluster.isLeader()) storage.beatPrimaries(System.currentTimeMillis());
            storage.probeReplicas(System.currentTimeMillis());
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, () -> "replica monitor round failed: " + e);
        }
    }

    @Override public synchronized void close() {
        if (scheduler != null) scheduler.shutdownNow();
        scheduler = null;
    }
}
