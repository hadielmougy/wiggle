package com.wiggle.server.store;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Transactions routed to a shard, with the transaction bound to the calling thread for the
 * {@link Repositories} to run on.
 *
 * <p>A transaction opened inside a write transaction on the same shard joins it: the body runs in the
 * outer transaction, on its connection. A write transaction is refused inside a write transaction on
 * another shard: it would commit on its own whatever became of the outer one. A read inside any
 * transaction on the same shard joins it too, and one on another shard runs in a transaction of its
 * own, as does a write inside a read.
 */
public final class ShardTransactions {

    private final Storage storage;

    public ShardTransactions(Storage storage) {
        this.storage = storage;
    }

    /** The shard the instance or token {@code id} lives on. */
    public int shardOf(String id) { return storage.shardOf(id); }

    /** The shard holding the cluster-global rows. */
    public int home() { return storage.home(); }

    /** The shard holding portal accounts and machine credentials. */
    public int auth() { return storage.auth(); }

    /** The shards that hold instances. */
    public List<Integer> instanceShards() { return storage.instanceShards(); }

    /** Runs {@code body} in a write transaction on {@code shard}. */
    public <R> R inShard(int shard, Supplier<R> body) {
        CurrentTx.Scope outer = CurrentTx.current();
        if (outer != null && outer.write()) {
            if (outer.shard() == shard) return body.get();
            throw new IllegalStateException("a write transaction on shard " + shard + " inside one on shard "
                    + outer.shard() + " would commit on its own; finish the first before opening the second");
        }
        return storage.inShard(shard, repositories -> CurrentTx.bind(repositories, shard, true, body));
    }

    public void inShard(int shard, Runnable body) {
        inShard(shard, () -> { body.run(); return null; });
    }

    /** {@link #inShard} on the shard holding the instance or token {@code id}. */
    public <R> R forInstance(String id, Supplier<R> body) { return inShard(shardOf(id), body); }

    public void forInstance(String id, Runnable body) { inShard(shardOf(id), body); }

    /** {@link #inShard} on the home shard. */
    public <R> R inHome(Supplier<R> body) { return inShard(home(), body); }

    public void inHome(Runnable body) { inShard(home(), body); }

    /** {@link #inShard} on the auth shard. */
    public <R> R inAuth(Supplier<R> body) { return inShard(auth(), body); }

    public void inAuth(Runnable body) { inShard(auth(), body); }

    /** Runs {@code body} in a read-only transaction on {@code shard}, served by a replica when {@code freshness} allows. */
    public <R> R read(int shard, Freshness freshness, Supplier<R> body) {
        CurrentTx.Scope outer = CurrentTx.current();
        if (outer != null && outer.shard() == shard) return body.get();
        return storage.readShard(shard, freshness, repositories -> CurrentTx.bind(repositories, shard, false, body));
    }

    /** {@link #read} on the primary of {@code shard}. */
    public <R> R read(int shard, Supplier<R> body) { return read(shard, Freshness.PRIMARY, body); }

    public void read(int shard, Runnable body) {
        read(shard, Freshness.PRIMARY, () -> { body.run(); return null; });
    }

    /** {@link #read} on the shard holding the instance or token {@code id}. */
    public <R> R readFor(String id, Freshness freshness, Supplier<R> body) { return read(shardOf(id), freshness, body); }

    /** {@link #read} on the primary of the home shard. */
    public <R> R readHome(Supplier<R> body) { return read(home(), body); }

    public void readHome(Runnable body) { read(home(), body); }

    /** {@link #read} on the primary of the auth shard. */
    public <R> R readAuth(Supplier<R> body) { return read(auth(), body); }

    /** {@link #read} on every instance shard in turn, the lists concatenated in shard order. */
    public <T> List<T> readEach(Supplier<List<T>> body) {
        List<T> out = new ArrayList<>();
        for (int shard : instanceShards()) out.addAll(read(shard, body));
        return out;
    }
}
