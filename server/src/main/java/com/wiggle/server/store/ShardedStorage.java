package com.wiggle.server.store;

import com.wiggle.core.InstanceStatus;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * A {@link Storage} over several databases, one {@link Storage} per shard. It only routes: every
 * transaction runs on the one shard its entry point names, and no transaction spans two.
 *
 * <p>An unrouted {@link #inTx} is refused, so a call site that never chose a shard fails instead of
 * writing to an arbitrary database.
 */
public final class ShardedStorage implements Storage {

    /**
     * One shard this store routes to.
     *
     * @param instances whether it holds instances, so that sweeps, claims and console reads visit it
     */
    public record Member(int id, ShardState state, boolean instances, Storage storage) {}

    private final Map<Integer, Storage> shards;
    private final List<Member> members;
    private final List<Integer> instanceShards;
    private final int home;
    /** Shards the registry records as retired, filled by {@link #migrate}. */
    private volatile Set<Integer> retired = Set.of();

    /** Every shard ACTIVE and holding instances, by its permanent id, in the order fan-out visits them. */
    public ShardedStorage(Map<Integer, Storage> shards, int home) {
        this(shards.entrySet().stream()
                .map(e -> new Member(e.getKey(), ShardState.ACTIVE, true, e.getValue())).toList(), home);
    }

    /**
     * @param members the shards, in the order fan-out visits them
     * @param home    the shard holding the cluster-global rows
     */
    public ShardedStorage(List<Member> members, int home) {
        if (members.isEmpty()) throw new IllegalArgumentException("a sharded store needs at least one shard");
        Map<Integer, Storage> byId = new LinkedHashMap<>();
        for (Member m : members) {
            if (m.id() < 0) throw new IllegalArgumentException("a shard id is not negative: " + m.id());
            if (byId.put(m.id(), m.storage()) != null) {
                throw new IllegalArgumentException("shard " + m.id() + " is listed twice");
            }
        }
        if (!byId.containsKey(home)) {
            throw new IllegalArgumentException("home shard " + home + " is not one of " + byId.keySet());
        }
        this.shards = byId;
        this.members = List.copyOf(members);
        this.instanceShards = members.stream()
                .filter(m -> m.instances() && m.state() != ShardState.RETIRED).map(Member::id).toList();
        if (instanceShards.isEmpty()) throw new IllegalArgumentException("no shard holds instances");
        this.home = home;
    }

    /** The shards, in the order fan-out visits them. */
    public List<Member> members() {
        return members;
    }

    /**
     * Migrates every shard, then binds this node's view of them to what the databases record:
     * <ul>
     *   <li>each database is claimed for its shard id the first time, and must name that id after;</li>
     *   <li>the registry on the home shard gains every new shard, and a shard's state only moves
     *       forward: ACTIVE, then DRAINING, then RETIRED, the last only once it holds no live
     *       instance;</li>
     *   <li>a shard the registry holds as not RETIRED must still be listed.</li>
     * </ul>
     * Any mismatch fails here, so a node refuses to start rather than route to the wrong database.
     */
    @Override public void migrate() {
        shards.values().forEach(Storage::migrate);
        for (Member m : members) {
            int claimed = inShard(m.id(), tx -> {
                tx.claimShardIdentity(m.id());
                return tx.shardIdentity().orElseThrow();
            });
            if (claimed != m.id()) {
                throw new IllegalStateException("shard " + m.id() + " points at the database of shard "
                        + claimed + "; check its primary url");
            }
        }
        retired = inHome(this::reconcileRegistry);
    }

    /** Brings the registry up to the listed states and returns the retired shard ids. */
    private Set<Integer> reconcileRegistry(Tx tx) {
        long now = System.currentTimeMillis();
        Map<Integer, Rows.ShardRecord> known = new LinkedHashMap<>();
        tx.shardRegistry().forEach(r -> known.put(r.shardId(), r));
        for (Member m : members) {
            Rows.ShardRecord r = known.get(m.id());
            if (r != null && m.state().ordinal() < r.state().ordinal()) {
                throw new IllegalStateException("shard " + m.id() + " is listed " + m.state() + " but the registry "
                        + "records it " + r.state() + "; a shard's state only moves forward");
            }
            if (r != null && r.state() == m.state()) continue;
            if (m.state() == ShardState.RETIRED) requireEmpty(m);
            Rows.ShardRecord next = new Rows.ShardRecord(m.id(), m.state(), r == null ? now : r.firstSeen(),
                    m.state() == ShardState.RETIRED ? Long.valueOf(now) : null);
            tx.putShardRecord(next);
            known.put(m.id(), next);
        }
        Set<Integer> out = new HashSet<>();
        for (Rows.ShardRecord r : known.values()) {
            if (r.state() == ShardState.RETIRED) {
                out.add(r.shardId());
            } else if (!shards.containsKey(r.shardId())) {
                throw new IllegalStateException("the registry records shard " + r.shardId() + " as " + r.state()
                        + ", but it is not listed; list it until it is drained and retired");
            }
        }
        return Set.copyOf(out);
    }

    private void requireEmpty(Member m) {
        int live = inShard(m.id(), tx -> {
            int n = 0;
            for (InstanceStatus s : InstanceStatus.values()) if (s.live()) n += tx.countInstances(s);
            return n;
        });
        if (live > 0) {
            throw new IllegalStateException("shard " + m.id() + " cannot be RETIRED: it still holds " + live
                    + " live instance(s); leave it DRAINING until they finish");
        }
    }

    @Override public <R> R inTx(Function<Tx, R> work) {
        throw new IllegalStateException("unrouted transaction on a sharded store: open it with inTxFor, "
                + "inShard, inHome, readFor or readShard");
    }

    @Override public int home() { return home; }

    @Override public List<Integer> instanceShards() { return instanceShards; }

    @Override public <R> R inShard(int shard, Function<Tx, R> work) {
        return shard(shard).inTx(work);
    }

    @Override public <R> R readShard(int shard, Freshness freshness, Function<ReadTx, R> work) {
        return shard(shard).readShard(shard, freshness, work);
    }

    /**
     * The store for {@code shard}. An unknown shard is a transient failure, never "not found": an id
     * minted by a node with a newer topology names a shard this node may not know yet.
     */
    private Storage shard(int shard) {
        if (retired.contains(shard)) throw new ShardRetiredException(shard);
        Storage s = shards.get(shard);
        if (s == null) {
            throw new StorageException("shard " + shard + " is not in this node's topology " + shards.keySet(),
                    null, StorageException.Classification.TRANSIENT);
        }
        return s;
    }

    /** The shard ids with each shard's own fingerprint, in order; null when any shard has none. */
    @Override public String fingerprint() {
        List<String> parts = new ArrayList<>();
        for (Map.Entry<Integer, Storage> e : shards.entrySet()) {
            String fp = e.getValue().fingerprint();
            if (fp == null) return null;
            parts.add(e.getKey() + "=" + fp);
        }
        return String.join(",", parts);
    }

    @Override public void close() {
        RuntimeException first = null;
        for (Storage s : shards.values()) {
            try {
                s.close();
            } catch (RuntimeException e) {
                if (first == null) first = e;
                else first.addSuppressed(e);
            }
        }
        if (first != null) throw first;
    }
}
