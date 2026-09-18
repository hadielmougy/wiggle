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

    /**
     * A stable identity of the underlying store, shared by every node of the same cell (they point at
     * the same database) and distinct across cells (different databases). The coordinator uses it to
     * reject two distinct cells that accidentally reuse a {@code cell_id}. Returns {@code null} when the
     * backend has no cross-node identity (e.g. in-memory, which cannot be shared between processes); the
     * coordinator then skips the guard for that node.
     */
    default String fingerprint() { return null; }

    @Override void close();
}
