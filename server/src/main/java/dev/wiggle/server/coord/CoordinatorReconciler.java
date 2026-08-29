package dev.wiggle.server.coord;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * Leader-only coordinator reconciliation. Like {@code Housekeeper}, only the leader runs the
 * clock-driven duties, so an N-node coordinator cluster does them once. Every write goes through
 * {@link CoordinatorStore}'s compare-and-set, so the tolerated brief-overlap election stays safe
 * (a stale ex-leader's write is rejected).
 *
 * <p>Phase 1/T6 duty: expire dead nodes from the roster. Later phases add ring drain/retire (T12).
 */
public final class CoordinatorReconciler implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(CoordinatorReconciler.class.getName());

    private final CoordinatorStore store;
    private final BooleanSupplier isLeader;
    private final long intervalMillis;
    private final long nodeDeadMillis;
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "wiggle-coord-reconciler");
                t.setDaemon(true);
                return t;
            });

    public CoordinatorReconciler(CoordinatorStore store, BooleanSupplier isLeader,
                                 long intervalMillis, long nodeDeadMillis) {
        this.store = store;
        this.isLeader = isLeader;
        this.intervalMillis = intervalMillis;
        this.nodeDeadMillis = nodeDeadMillis;
    }

    public void start() {
        long period = Math.max(200, intervalMillis);
        scheduler.scheduleAtFixedRate(this::tick, period, period, TimeUnit.MILLISECONDS);
    }

    /** Package-visible so tests can drive a tick deterministically. */
    void tick() {
        if (!isLeader.getAsBoolean()) {
            LOG.log(System.Logger.Level.DEBUG, "coord reconcile: skipped, not leader");
            return;
        }
        try {
            int expired = store.expireNodes(System.currentTimeMillis() - nodeDeadMillis);
            if (expired > 0) LOG.log(System.Logger.Level.INFO, () -> "coord reconcile: expired " + expired + " dead node(s)");
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "coord reconcile failed: " + e);
        }
    }

    @Override public void close() {
        scheduler.shutdownNow();
    }
}
