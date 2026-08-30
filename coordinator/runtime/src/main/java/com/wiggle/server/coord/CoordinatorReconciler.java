package com.wiggle.server.coord;

import com.wiggle.server.coord.CoordPolicy.EpochRing;
import com.wiggle.server.coord.CoordPolicy.EpochStatus;

import java.util.LinkedHashMap;
import java.util.Map;
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
 * <p>Duties: expire dead nodes from the roster (T6), and retire drained epochs (T12/R21) -- a DRAINING
 * epoch whose live-instance count has reached zero on every cell (per the {@link LiveCensus} fed by node
 * heartbeats) is marked RETIRED, which bumps the policy generation so nodes stop polling it.
 */
public final class CoordinatorReconciler implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(CoordinatorReconciler.class.getName());
    private static final int CAS_ATTEMPTS = 5;

    private final CoordinatorStore store;
    private final LiveCensus census;
    private final BooleanSupplier isLeader;
    private final long intervalMillis;
    private final long nodeDeadMillis;
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "wiggle-coord-reconciler");
                t.setDaemon(true);
                return t;
            });

    public CoordinatorReconciler(CoordinatorStore store, LiveCensus census, BooleanSupplier isLeader,
                                 long intervalMillis, long nodeDeadMillis) {
        this.store = store;
        this.census = census;
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
            census.prune(System.currentTimeMillis() - nodeDeadMillis);
            for (CoordPolicy p : store.listPolicies()) retireDrained(p.namespace());
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "coord reconcile failed: " + e);
        }
    }

    /**
     * Retires every DRAINING epoch of {@code namespace} that the census confirms has zero live
     * instances. Only acts on a fresh census (a silent namespace is never retired -- the safe
     * direction). CAS-guarded and retried, so a losing race just retries next tick.
     */
    void retireDrained(String namespace) {
        long freshSince = System.currentTimeMillis() - nodeDeadMillis;
        for (int attempt = 0; attempt < CAS_ATTEMPTS; attempt++) {
            CoordPolicy c = store.getPolicy(namespace).orElse(null);
            if (c == null) return;
            LiveCensus.Aggregate live = census.aggregate(namespace, freshSince);
            if (!live.hasFresh()) return;   // no recent report -> no confirmation of zero -> do not retire

            Map<Long, EpochRing> epochs = new LinkedHashMap<>(c.epochs());
            boolean changed = false;
            for (Map.Entry<Long, EpochRing> e : c.epochs().entrySet()) {
                EpochRing er = e.getValue();
                if (er.status() == EpochStatus.DRAINING && live.count(e.getKey()) == 0) {
                    epochs.put(e.getKey(), new EpochRing(er.ring(), EpochStatus.RETIRED));
                    changed = true;
                }
            }
            if (!changed) return;
            if (store.casPolicy(namespace, c.revision(),
                    new CoordPolicy(namespace, c.currentEpoch(), 0, epochs)) > 0) {
                LOG.log(System.Logger.Level.INFO, () -> "coord reconcile: retired drained epoch(s) of '" + namespace + "'");
                return;
            }
        }
    }

    @Override public void close() {
        scheduler.shutdownNow();
    }
}
