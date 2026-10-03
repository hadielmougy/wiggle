package com.wiggle.server.store;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * One shard's primary and its read replicas. Every write and every {@link Freshness#PRIMARY} read
 * goes to the primary; a {@link Freshness#REPLICA_OK} read goes to a healthy replica in turn.
 *
 * <p>A replica is healthy when its last probe answered and its lag -- how far the replicated
 * heartbeat trails this node's clock -- is within {@code maxLagMillis}. A replica that fails a probe
 * or a read is left out and probed again after a backoff that doubles up to
 * {@link #MAX_BACKOFF_MILLIS}. With no healthy replica, {@link Fallback} decides.
 */
public final class ReplicatedStorage implements Storage {

    private static final System.Logger LOG = System.getLogger(ReplicatedStorage.class.getName());

    static final long MIN_BACKOFF_MILLIS = 1_000;
    static final long MAX_BACKOFF_MILLIS = 30_000;

    /** What a replica-allowed read does when no replica is healthy. */
    public enum Fallback {
        /** Read from the primary. */
        PRIMARY,
        /** Refuse the read as a transient failure, so a caller sees UNAVAILABLE and the primary is spared. */
        FAIL
    }

    /** A replica as this node last measured it. {@code lagMillis} is -1 when unknown. */
    public record ReplicaStatus(String name, boolean healthy, long lagMillis, String lastError) {}

    private static final class Replica {
        final String name;
        final Storage store;
        volatile boolean healthy;
        volatile long lagMillis = -1;
        volatile long nextProbeAt;
        volatile long backoff;
        volatile String lastError;

        Replica(String name, Storage store) {
            this.name = name;
            this.store = store;
        }
    }

    private final int shard;
    private final Storage primary;
    private final List<Replica> replicas;
    private final long maxLagMillis;
    private final Fallback fallback;
    private final AtomicInteger turn = new AtomicInteger();
    private final LongSupplier clock;

    /**
     * @param shard    the shard id this database is claimed for; 0 for a deployment on one database
     * @param replicas each replica by a name for logs, opened read-only
     */
    public ReplicatedStorage(int shard, Storage primary, List<Named> replicas, long maxLagMillis, Fallback fallback) {
        this(shard, primary, replicas, maxLagMillis, fallback, System::currentTimeMillis);
    }

    /** As above, with the clock a failed read is timed by, for its probe backoff. */
    ReplicatedStorage(int shard, Storage primary, List<Named> replicas, long maxLagMillis, Fallback fallback,
                      LongSupplier clock) {
        this.clock = clock;
        if (maxLagMillis < 0) throw new IllegalArgumentException("maxLagMillis is not negative: " + maxLagMillis);
        this.shard = shard;
        this.primary = primary;
        this.replicas = replicas.stream().map(r -> new Replica(r.name(), r.store())).toList();
        this.maxLagMillis = maxLagMillis;
        this.fallback = fallback;
    }

    /** A replica store and the name logs call it by. */
    public record Named(String name, Storage store) {}

    /** Migrates the primary and claims it for this shard, so its heartbeat row exists. */
    @Override public void migrate() {
        primary.migrate();
        primary.inTx(tx -> {
            tx.claimShardIdentity(shard);
            return null;
        });
    }

    @Override public <R> R inTx(Function<Tx, R> work) {
        return primary.inTx(work);
    }

    @Override public <R> R readShard(int shard, Freshness freshness, Function<ReadTx, R> work) {
        if (freshness == Freshness.PRIMARY || replicas.isEmpty()) return primary.inTx(work::apply);
        Replica r = pick();
        if (r != null) {
            try {
                return r.store.inTx(work::apply);
            } catch (RuntimeException e) {
                eject(r, clock.getAsLong(), e);
            }
        }
        if (fallback == Fallback.FAIL) {
            throw new StorageException("no read replica of shard " + this.shard + " is within "
                    + maxLagMillis + " ms; replica reads are not served from the primary", null,
                    StorageException.Classification.TRANSIENT);
        }
        return primary.inTx(work::apply);
    }

    /** The next healthy replica in turn, or null when none is. */
    private Replica pick() {
        int n = replicas.size();
        int start = Math.floorMod(turn.getAndIncrement(), n);
        for (int i = 0; i < n; i++) {
            Replica r = replicas.get((start + i) % n);
            if (r.healthy) return r;
        }
        return null;
    }

    @Override public boolean hasReplicas() {
        return !replicas.isEmpty();
    }

    @Override public void beatPrimaries(long now) {
        primary.inTx(tx -> {
            tx.writeShardBeat(now);
            return null;
        });
    }

    @Override public void probeReplicas(long now) {
        for (Replica r : replicas) {
            if (now < r.nextProbeAt) continue;
            try {
                OptionalLong beat = r.store.inTx(tx -> tx.shardBeat());
                long lag = beat.isPresent() ? Math.max(0, now - beat.getAsLong()) : -1;
                boolean healthy = lag >= 0 && lag <= maxLagMillis;
                if (healthy != r.healthy) {
                    LOG.log(System.Logger.Level.INFO, () -> "replica " + r.name + " of shard " + shard
                            + (healthy ? " is serving reads" : " is not serving reads: "
                            + (lag < 0 ? "no heartbeat yet" : "lag " + lag + " ms over " + maxLagMillis)));
                }
                r.lagMillis = lag;
                r.healthy = healthy;
                r.backoff = 0;
                r.lastError = null;
            } catch (RuntimeException e) {
                eject(r, now, e);
            }
        }
    }

    private void eject(Replica r, long now, RuntimeException e) {
        boolean was = r.healthy;
        r.healthy = false;
        r.backoff = Math.min(MAX_BACKOFF_MILLIS, Math.max(MIN_BACKOFF_MILLIS, r.backoff * 2));
        r.nextProbeAt = now + r.backoff;
        r.lastError = String.valueOf(e.getMessage());
        if (was) {
            LOG.log(System.Logger.Level.WARNING, () -> "replica " + r.name + " of shard " + shard
                    + " failed and is not serving reads; probing again in " + r.backoff + " ms: " + e);
        }
    }

    /** Each replica as last measured, in configured order. */
    public List<ReplicaStatus> replicaStatus() {
        List<ReplicaStatus> out = new ArrayList<>();
        for (Replica r : replicas) out.add(new ReplicaStatus(r.name, r.healthy, r.lagMillis, r.lastError));
        return out;
    }

    @Override public String fingerprint() {
        return primary.fingerprint();
    }

    @Override public void close() {
        RuntimeException first = null;
        for (Storage s : allStores()) {
            try {
                s.close();
            } catch (RuntimeException e) {
                if (first == null) first = e;
                else first.addSuppressed(e);
            }
        }
        if (first != null) throw first;
    }

    private List<Storage> allStores() {
        List<Storage> all = new ArrayList<>();
        all.add(primary);
        replicas.forEach(r -> all.add(r.store));
        return all;
    }
}
