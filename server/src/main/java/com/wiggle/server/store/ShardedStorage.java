package com.wiggle.server.store;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

    @Override public void migrate() {
        shards.values().forEach(Storage::migrate);
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
