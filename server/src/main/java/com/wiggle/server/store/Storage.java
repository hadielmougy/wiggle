package com.wiggle.server.store;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Storage SPI for the cell engine. Every engine mutation runs inside {@link #inTx}; implementations
 * must make that unit atomic and must honour {@link Tx#lockInstance} as a mutual-exclusion point for a
 * single workflow instance. The engine knows nothing of the coordinator; a storage adapter that can
 * also back a coordinator exposes that separately via {@code CoordinatorStoreProvider}.
 */
public interface Storage extends AutoCloseable {

    /** Applies the cell schema ({@code wf_*} tables). */
    void migrate();

    <R> R inTx(Function<Tx, R> work);

    default void inTxVoid(Consumer<Tx> work) {
        inTx(tx -> { work.accept(tx); return null; });
    }

    @Override void close();
}
