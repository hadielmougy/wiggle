package com.wiggle.server.engine;

import com.wiggle.core.WorkflowVersion;
import com.wiggle.core.criteria.TaskFacts;
import com.wiggle.server.store.ShardTransactions;
import com.wiggle.server.store.Storage;

import java.util.List;
import java.util.function.Supplier;

/**
 * The engine's transaction scope: a {@link ShardTransactions} transaction plus the wake-on-produce
 * signal that must fire after it commits. A queue marked by {@link #wake} during the transaction is
 * signalled to the {@link DispatchNotifier} once -- and only once -- the work is durable, so a woken
 * poller never claims against uncommitted state.
 *
 * <p>Every transaction is routed: to the shard an instance or token id carries, to one shard, to the
 * home shard, or to each instance shard in turn. The transaction is bound to the calling thread, so a
 * body works through {@link com.wiggle.server.store.Repositories}.
 *
 * <p>A write scope opened inside another on the same shard joins it, and its queues are woken with
 * the outer's; one on another shard is refused (see {@link ShardTransactions}).
 */
final class Transactions {

    private final ShardTransactions shards;
    private final DispatchNotifier notifier;

    /** The tokens the in-flight transaction parked READY. */
    private final ThreadLocal<DispatchNotifier.Batch> pending = new ThreadLocal<>();

    Transactions(Storage storage, DispatchNotifier notifier) {
        this.shards = new ShardTransactions(storage);
        this.notifier = notifier;
    }

    /** The routed transactions this scope opens. */
    ShardTransactions shards() {
        return shards;
    }

    /** Marks {@code queue} for the post-commit wake-on-produce notification, for a token of
     *  {@code version} with {@code facts} (each null when not known); a null queue is a no-op. Handed
     *  to producers as a {@link QueueWake}. */
    void wake(String queue, WorkflowVersion version, TaskFacts facts) {
        DispatchNotifier.Batch marked = pending.get();
        if (marked != null) marked.ready(queue, version, facts);
    }

    /** The shard the instance or token {@code id} lives on. */
    int shardOf(String id) {
        return shards.shardOf(id);
    }

    /** The shard holding the cluster-global rows. */
    int home() {
        return shards.home();
    }

    /** The shards that hold instances. */
    List<Integer> instanceShards() {
        return shards.instanceShards();
    }

    /** Runs {@code body} on the shard holding {@code id}, then (post-commit) wakes pollers for any
     *  queue it marked. */
    <T> T forInstance(String id, Supplier<T> body) {
        return inShard(shards.shardOf(id), body);
    }

    void forInstance(String id, Runnable body) {
        forInstance(id, () -> {
            body.run();
            return null;
        });
    }

    /** {@link #forInstance(String, Supplier)} on the home shard. */
    <T> T inHome(Supplier<T> body) {
        return inShard(shards.home(), body);
    }

    /** Runs {@code body} on {@code shard}, then (post-commit) wakes pollers for any queue it marked. */
    <T> T inShard(int shard, Supplier<T> body) {
        if (pending.get() != null) return shards.inShard(shard, body);   // joins the outer scope, or is refused
        DispatchNotifier.Batch mine = notifier.batch();
        pending.set(mine);
        T result;
        try {
            result = shards.inShard(shard, () -> {
                mine.clear();   // a replayed attempt counts its own tokens, not the rolled-back one's too
                return body.get();
            });
        } finally {
            pending.remove();
        }
        mine.signal();   // post-commit: an exception above never reaches here
        return result;
    }

    /** A transaction with no wake-on-produce scope -- reads, and claims that park nothing READY -- on
     *  the shard holding {@code id}. */
    <T> T read(String id, Supplier<T> body) {
        return readShard(shards.shardOf(id), body);
    }

    /** {@link #read} on {@code shard}. */
    <T> T readShard(int shard, Supplier<T> body) {
        return shards.read(shard, body);
    }

    /** {@link #read} on the home shard. */
    <T> T readHome(Supplier<T> body) {
        return readShard(shards.home(), body);
    }

    void readHomeVoid(Runnable body) {
        readHome(() -> {
            body.run();
            return null;
        });
    }

    /** {@link #read} on every instance shard in turn, the lists concatenated in shard order. */
    <T> List<T> readEach(Supplier<List<T>> body) {
        return shards.readEach(body);
    }

    /** {@link #read} on every instance shard in turn, the counts summed. */
    int sumEach(Supplier<Integer> body) {
        int total = 0;
        for (int shard : shards.instanceShards()) total += readShard(shard, body);
        return total;
    }

}
