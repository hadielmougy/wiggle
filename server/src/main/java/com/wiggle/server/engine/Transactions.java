package com.wiggle.server.engine;

import com.wiggle.server.store.Storage;
import com.wiggle.server.store.Tx;

import java.util.HashMap;
import java.util.Map;

/**
 * The engine's transaction scope: a storage transaction plus the wake-on-produce signal that must
 * fire after it commits. A queue marked by {@link #wake} during the transaction is signalled to
 * the {@link DispatchNotifier} once -- and only once -- the work is durable, so a woken poller
 * never claims against uncommitted state.
 *
 * <p>Scopes do not nest, and {@link #inTx} refuses to open one inside another. A storage
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

    /** Runs {@code body} in a transaction, then (post-commit) wakes pollers for any queue it marked. */
    <T> T inTx(TxBody<T> body) {
        if (readyQueues.get() != null) {
            throw new IllegalStateException("nested transaction scope: this thread is already inside "
                    + "inTx. A nested call would run on a second connection and deadlock against the "
                    + "instance lock the outer transaction holds. Pass the open Tx down instead.");
        }
        Map<String, Integer> mine = new HashMap<>();
        readyQueues.set(mine);
        T result;
        try {
            result = storage.inTx(tx -> {
                mine.clear();   // a replayed attempt counts its own tokens, not the rolled-back one's too
                return body.run(tx);
            });
        } finally {
            readyQueues.remove();
        }
        notifier.signal(mine);   // post-commit: an exception above never reaches here
        return result;
    }

    /** {@link #inTx} for a body with no return value. */
    void inTxVoid(TxWork body) {
        inTx(tx -> {
            body.run(tx);
            return null;
        });
    }

    /** A transaction with no wake-on-produce scope: reads, and claims that park nothing READY. */
    <T> T read(TxBody<T> body) {
        return storage.inTx(body::run);
    }

    /** {@link #read} for a body with no return value. */
    void readVoid(TxWork body) {
        storage.inTxVoid(body::run);
    }
}
