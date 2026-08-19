package dev.wiggle.server.store;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Storage SPI. Every engine mutation runs inside {@link #inTx}; implementations must
 * make that unit atomic and must honour {@link Tx#lockInstance} as a mutual-exclusion
 * point for a single workflow instance.
 */
public interface Storage extends AutoCloseable {

    void migrate();

    <R> R inTx(Function<Tx, R> work);

    default void inTxVoid(Consumer<Tx> work) {
        inTx(tx -> { work.accept(tx); return null; });
    }

    @Override void close();
}
