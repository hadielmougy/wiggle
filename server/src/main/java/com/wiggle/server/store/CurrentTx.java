package com.wiggle.server.store;

import java.util.function.Supplier;

/**
 * The transaction open on the calling thread, as {@link ShardTransactions} opened it: what the
 * {@link Repositories} run on. A body bound inside another restores the outer one when it ends.
 */
public final class CurrentTx {

    private CurrentTx() {}

    /** One bound transaction: its repositories, its shard, and whether it may write. */
    record Scope(ReadRepositories repositories, int shard, boolean write) { }

    private static final ThreadLocal<Scope> CURRENT = new ThreadLocal<>();

    static Scope current() {
        return CURRENT.get();
    }

    /** Runs {@code body} with {@code repositories} bound as the calling thread's transaction on {@code shard}. */
    static <R> R bind(ReadRepositories repositories, int shard, boolean write, Supplier<R> body) {
        Scope outer = CURRENT.get();
        CURRENT.set(new Scope(repositories, shard, write));
        try {
            return body.get();
        } finally {
            if (outer == null) CURRENT.remove();
            else CURRENT.set(outer);
        }
    }

    /** The repositories of the calling thread's transaction; refused when none is open. */
    public static ReadRepositories require() {
        Scope scope = CURRENT.get();
        if (scope == null) throw new IllegalStateException("no transaction is open on this thread; open one through ShardTransactions");
        return scope.repositories();
    }

    /** The repositories of the calling thread's write transaction; refused when none, or only a read, is open. */
    public static WriteRepositories requireWrite() {
        Scope scope = CURRENT.get();
        if (scope == null || !scope.write()) throw new IllegalStateException("no write transaction is open on this thread");
        return (WriteRepositories) scope.repositories();
    }
}
