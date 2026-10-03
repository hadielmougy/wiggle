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

    private final Map<Integer, Storage> shards;
    private final List<Integer> instanceShards;
    private final int home;

    /**
     * @param shards every shard by its permanent id, in the order fan-out visits them; each one holds
     *               instances
     * @param home   the shard holding the cluster-global rows
     */
    public ShardedStorage(Map<Integer, Storage> shards, int home) {
        if (shards.isEmpty()) throw new IllegalArgumentException("a sharded store needs at least one shard");
        if (!shards.containsKey(home)) {
            throw new IllegalArgumentException("home shard " + home + " is not one of " + shards.keySet());
        }
        shards.keySet().forEach(id -> {
            if (id < 0) throw new IllegalArgumentException("a shard id is not negative: " + id);
        });
        this.shards = new LinkedHashMap<>(shards);
        this.instanceShards = List.copyOf(this.shards.keySet());
        this.home = home;
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
