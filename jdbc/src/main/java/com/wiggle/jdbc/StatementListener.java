package com.wiggle.jdbc;

/**
 * Told of every round trip a {@link JdbcStorage} makes while it is attached with
 * {@link JdbcStorage#observe}: each statement execution (a batch counts once) and each commit and
 * rollback, on the thread that issued it, in the order issued. Called inside the transaction, so it
 * must be quick and must not throw.
 */
public interface StatementListener {

    /** A statement of {@code sql} was executed. */
    void executed(String sql);

    /** A transaction was committed. */
    void committed();

    /** A transaction was rolled back. */
    default void rolledBack() { }
}
