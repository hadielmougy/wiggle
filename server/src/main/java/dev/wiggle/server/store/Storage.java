package dev.wiggle.server.store;

import dev.wiggle.server.ServerRole;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Storage SPI. Every engine mutation runs inside {@link #inTx}; implementations must
 * make that unit atomic and must honour {@link Tx#lockInstance} as a mutual-exclusion
 * point for a single workflow instance.
 */
public interface Storage extends AutoCloseable {

    /** Applies the cell schema. Equivalent to {@link #migrate(ServerRole) migrate(ServerRole.CELL)}. */
    void migrate();

    /**
     * Applies the schema for {@code role}: cell tables ({@code wf_*}) or coordinator tables
     * ({@code coord_*}). The default ignores the role and applies the cell schema, so stores that do
     * not host a coordinator (in-memory, and backends not yet coordinator-aware) keep working; a
     * JDBC store overrides this to select the right migration set (and to refuse a database whose
     * baseline belongs to the other role).
     */
    default void migrate(ServerRole role) {
        migrate();
    }

    <R> R inTx(Function<Tx, R> work);

    default void inTxVoid(Consumer<Tx> work) {
        inTx(tx -> { work.accept(tx); return null; });
    }

    @Override void close();
}
