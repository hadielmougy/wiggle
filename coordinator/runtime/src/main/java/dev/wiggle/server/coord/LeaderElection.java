package dev.wiggle.server.coord;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The coordinator's leader election (option A): a periodic lease renewal over the {@link CoordinatorStore}.
 * Each tick tries to acquire/renew the single coordinator lease; {@link #isLeader()} reflects whether this
 * node currently holds it. Leader-only duties (the reconcile/retire loop) gate on it, so a durable,
 * atomic {@code acquireLeadership} (JDBC CAS / Cassandra LWT) keeps a multi-node coordinator single-writer.
 * A single coordinator (or an in-memory store) simply holds the lease continuously.
 */
public final class LeaderElection implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(LeaderElection.class.getName());

    /** Lease longer than a few renew intervals, so a brief GC/pause doesn't drop leadership. */
    public static final long LEASE_MILLIS = 15_000;
    public static final long RENEW_MILLIS = 5_000;

    private final CoordinatorStore store;
    private final String nodeId;
    private final long leaseMillis;
    private final long renewMillis;
    private volatile boolean leader;
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "wiggle-coord-election");
                t.setDaemon(true);
                return t;
            });

    public LeaderElection(CoordinatorStore store, String nodeId) {
        this(store, nodeId, LEASE_MILLIS, RENEW_MILLIS);
    }

    public LeaderElection(CoordinatorStore store, String nodeId, long leaseMillis, long renewMillis) {
        this.store = store;
        this.nodeId = nodeId;
        this.leaseMillis = leaseMillis;
        this.renewMillis = renewMillis;
    }

    /** Runs one acquire immediately (so {@link #isLeader()} is meaningful on return) then renews on a timer. */
    public LeaderElection start() {
        tick();
        long period = Math.max(500, renewMillis);
        scheduler.scheduleAtFixedRate(this::tick, period, period, TimeUnit.MILLISECONDS);
        return this;
    }

    private void tick() {
        try {
            boolean was = leader;
            leader = store.acquireLeadership(nodeId, System.currentTimeMillis(), leaseMillis);
            if (leader != was) {
                LOG.log(System.Logger.Level.INFO, () -> "coordinator '" + nodeId + "' "
                        + (leader ? "acquired" : "lost") + " leadership");
            }
        } catch (RuntimeException e) {
            leader = false;   // on any store error, step down; the winner's lease still fences writes
            LOG.log(System.Logger.Level.WARNING, "coordinator leader election tick failed: " + e);
        }
    }

    public boolean isLeader() {
        return leader;
    }

    @Override public void close() {
        scheduler.shutdownNow();
        if (leader) {
            try { store.releaseLeadership(nodeId); } catch (RuntimeException ignored) { /* lease expires on its own */ }
        }
    }
}
