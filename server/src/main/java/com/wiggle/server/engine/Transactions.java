package com.wiggle.server.engine;

import com.wiggle.server.store.Storage;
import com.wiggle.server.store.Tx;

import java.util.HashSet;
import java.util.Set;

/**
 * The engine's transaction scope: a storage transaction plus the wake-on-produce signal that must
 * fire after it commits. A queue marked by {@link #wake} during the transaction is signalled to
 * the {@link DispatchNotifier} once -- and only once -- the work is durable, so a woken poller
 * never claims against uncommitted state. Nesting is safe: an inner scope hands its queues to the
 * outermost, which signals after its own commit.
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

    /** Queues that had a token parked READY during the in-flight transaction. */
    private final ThreadLocal<Set<String>> readyQueues = new ThreadLocal<>();

    Transactions(Storage storage, DispatchNotifier notifier) {
        this.storage = storage;
        this.notifier = notifier;
    }

    /** Runs {@code body} in a transaction, then (post-commit) wakes pollers for any queue it marked. */
    <T> T inTx(TxBody<T> body) {
        Set<String> outer = readyQueues.get();
        Set<String> mine = new HashSet<>();
        readyQueues.set(mine);
        T result;
        try {
            result = storage.inTx(body::run);
        } finally {
            readyQueues.set(outer);
        }
        if (outer != null) outer.addAll(mine);   // let the outermost scope signal, post its commit
        else notifier.signal(mine);
        return result;
    }

    /** {@link #inTx} for a body with no return value. */
    void inTxVoid(TxWork body) {
        Set<String> outer = readyQueues.get();
        Set<String> mine = new HashSet<>();
        readyQueues.set(mine);
        try {
            storage.inTxVoid(body::run);
        } finally {
            readyQueues.set(outer);
        }
        if (outer != null) outer.addAll(mine);
        else notifier.signal(mine);
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
