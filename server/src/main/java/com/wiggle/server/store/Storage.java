package com.wiggle.server.store;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Storage SPI for the engine. Every engine mutation runs inside {@link #inTx}; implementations
 * must make that unit atomic and must honour {@link Tx#lockInstance} as a mutual-exclusion point for a
 * single workflow instance.
 */
public interface Storage extends AutoCloseable {

    /** Applies the engine schema ({@code wf_*} tables). */
    void migrate();

    <R> R inTx(Function<Tx, R> work);

    default void inTxVoid(Consumer<Tx> work) {
        inTx(tx -> { work.accept(tx); return null; });
    }

    /**
     * A stable identity of the underlying store: the same for every node pointed at the same
     * database, and different across databases. Returns {@code null} when the backend has no
     * cross-node identity (e.g. in-memory, which cannot be shared between processes).
     */
    default String fingerprint() { return null; }

    @Override void close();
}
