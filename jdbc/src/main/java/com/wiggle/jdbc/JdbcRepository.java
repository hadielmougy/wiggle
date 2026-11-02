package com.wiggle.jdbc;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * A repository over one {@link JdbcStorage}. Its statements run on the connection of the transaction
 * open on that store on the calling thread, and refuse to run without one. Each runs after the
 * writes that transaction holds back ({@link JdbcStorage#bufferWrites}).
 */
abstract class JdbcRepository {

    final JdbcStorage owner;
    final Dialect dialect;

    JdbcRepository(JdbcStorage owner) {
        this.owner = owner;
        this.dialect = owner.dialect();
    }

    /** Statements with {@code :name} parameters, in the current transaction, after the writes it holds back. */
    final NamedParameterJdbcTemplate jdbc() {
        owner.requireTransaction();
        owner.flushPending();
        return owner.named();
    }

    /** Statements with positional {@code ?} parameters, in the current transaction, after the writes it holds back. */
    final JdbcTemplate positional() {
        owner.requireTransaction();
        owner.flushPending();
        return owner.named().getJdbcTemplate();
    }

    /** Whether {@code e} is a duplicate-key failure, as the dialect recognises one anywhere in the cause chain. */
    final boolean isDuplicateKey(RuntimeException e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof java.sql.SQLException sql && dialect.isDuplicateKey(sql)) return true;
        }
        return false;
    }

    /** Refuses a batch in which any statement touched a number of rows other than one. */
    static void requireOneRowEach(int[] counts, String what) {
        for (int c : counts) {
            if (c != 1 && c != java.sql.Statement.SUCCESS_NO_INFO) {
                throw new IllegalStateException(what + ": batched statement touched " + c
                        + " rows where exactly 1 was expected");
            }
        }
    }

    static Params params() {
        return new Params();
    }

    /** The first row of a query on a unique key, or empty. */
    static <T> java.util.Optional<T> first(java.util.List<T> rows) {
        return rows.isEmpty() ? java.util.Optional.empty() : java.util.Optional.of(rows.getFirst());
    }

    /** An attribute's number as the decimal it is stored as. */
    static java.math.BigDecimal attributeNumber(Number n) {
        return n instanceof java.math.BigDecimal d ? d : java.math.BigDecimal.valueOf(n.longValue());
    }
}
