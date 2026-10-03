package com.wiggle.server.engine;

import com.wiggle.server.store.Storage;
import com.wiggle.server.store.Tx;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The engine's transaction scope: a storage transaction plus the wake-on-produce signal that must
 * fire after it commits. A queue marked by {@link #wake} during the transaction is signalled to
 * the {@link DispatchNotifier} once -- and only once -- the work is durable, so a woken poller
 * never claims against uncommitted state.
 *
 * <p>Every transaction is routed: to the shard an instance or token id carries, to one shard, to the
 * home shard, or to each instance shard in turn.
 *
 * <p>Scopes do not nest, and {@link #inShard} refuses to open one inside another. A storage
 * transaction is a borrowed connection, so a nested call would be a second connection running an
 * independent transaction: it would commit on its own regardless of the outer one, it could not
 * see the outer's uncommitted writes, and -- since the engine takes instance row locks -- it would
 * block on a lock the outer holds and deadlock against itself. Failing loudly at the first nested
 * call is the only useful behaviour available.
 */
final class Transactions {

    /** A transaction body that produces a value. */
    @FunctionalInterface
    interface TxBody<T> {
        T run(Tx tx);
    }

    /** A transaction body that produces nothing. */
    @FunctionalInterface
    interface TxWork {
        void run(Tx tx);
    }

    private final Storage storage;
    private final DispatchNotifier notifier;

    /** How many tokens the in-flight transaction parked READY, by queue. */
    private final ThreadLocal<Map<String, Integer>> readyQueues = new ThreadLocal<>();

    Transactions(Storage storage, DispatchNotifier notifier) {
        this.storage = storage;
        this.notifier = notifier;
    }

    /** Marks {@code queue} for the post-commit wake-on-produce notification; null is a no-op.
     *  Handed to producers as a {@link QueueWake}. */
    void wake(String queue) {
        Map<String, Integer> ready = readyQueues.get();
        if (ready != null && queue != null) ready.merge(queue, 1, Integer::sum);
    }

    /** The shard the instance or token {@code id} lives on. */
    int shardOf(String id) {
        return storage.shardOf(id);
    }

    /** The shard holding the cluster-global rows. */
    int home() {
        return storage.home();
    }

    /** The shards that hold instances. */
    List<Integer> instanceShards() {
        return storage.instanceShards();
    }

    /** Runs {@code body} on the shard holding {@code id}, then (post-commit) wakes pollers for any
     *  queue it marked. */
    <T> T inTx(String id, TxBody<T> body) {
        return inShard(storage.shardOf(id), body);
    }

    /** {@link #inTx(String, TxBody)} for a body with no return value. */
    void inTxVoid(String id, TxWork body) {
        inTx(id, tx -> {
            body.run(tx);
            return null;
        });
    }

    /** {@link #inTx(String, TxBody)} on the home shard. */
    <T> T inHome(TxBody<T> body) {
        return inShard(storage.home(), body);
    }

    /** Runs {@code body} on {@code shard}, then (post-commit) wakes pollers for any queue it marked. */
    <T> T inShard(int shard, TxBody<T> body) {
        if (readyQueues.get() != null) {
            throw new IllegalStateException("nested transaction scope: this thread is already inside "
                    + "inTx. A nested call would run on a second connection and deadlock against the "
                    + "instance lock the outer transaction holds. Pass the open Tx down instead.");
        }
        Map<String, Integer> mine = new HashMap<>();
        readyQueues.set(mine);
        T result;
        try {
            result = storage.inShard(shard, tx -> {
                mine.clear();   // a replayed attempt counts its own tokens, not the rolled-back one's too
                return body.run(tx);
            });
        } finally {
            readyQueues.remove();
        }
        notifier.signal(mine);   // post-commit: an exception above never reaches here
        return result;
    }

    /** A transaction with no wake-on-produce scope -- reads, and claims that park nothing READY -- on
     *  the shard holding {@code id}. */
    <T> T read(String id, TxBody<T> body) {
        return readShard(storage.shardOf(id), body);
    }

    /** {@link #read} on {@code shard}. */
    <T> T readShard(int shard, TxBody<T> body) {
        return storage.inShard(shard, body::run);
    }

    /** {@link #read} on the home shard. */
    <T> T readHome(TxBody<T> body) {
        return readShard(storage.home(), body);
    }

    /** {@link #readHome} for a body with no return value. */
    void readHomeVoid(TxWork body) {
        readHome(tx -> {
            body.run(tx);
            return null;
        });
    }

    /** {@link #read} on every instance shard in turn, the lists concatenated in shard order. */
    <T> List<T> readEach(TxBody<List<T>> body) {
        List<T> out = new ArrayList<>();
        for (int shard : storage.instanceShards()) out.addAll(readShard(shard, body));
        return out;
    }

    /** {@link #read} on every instance shard in turn, the counts summed. */
    int sumEach(TxBody<Integer> body) {
        int total = 0;
        for (int shard : storage.instanceShards()) total += readShard(shard, body);
        return total;
    }
}
