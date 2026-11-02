package com.wiggle.tests;

import com.wiggle.server.store.ShardTransactions;
import com.wiggle.server.store.Storage;

import java.util.function.Supplier;

/** A write transaction on a test's store, on its home shard, through {@link ShardTransactions}, for tests that read and write rows directly. */
public final class TestTx {

    private TestTx() {}

    public static <R> R inHome(Storage storage, Supplier<R> body) {
        return new ShardTransactions(storage).inHome(body);
    }

    public static void inHome(Storage storage, Runnable body) {
        new ShardTransactions(storage).inHome(body);
    }
}
