package com.wiggle.jdbc;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * A repository over one {@link JdbcStorage}. Its statements run on the connection of the transaction
 * open on that store on the calling thread, and refuse to run without one.
 */
abstract class JdbcRepository {

    final JdbcStorage owner;
    final Dialect dialect;

    JdbcRepository(JdbcStorage owner) {
        this.owner = owner;
        this.dialect = owner.dialect();
    }

    /** Statements with {@code :name} parameters, in the current transaction. */
    final NamedParameterJdbcTemplate jdbc() {
        owner.requireTransaction();
        return owner.named();
    }

    /** Statements with positional {@code ?} parameters, in the current transaction. */
    final JdbcTemplate positional() {
        owner.requireTransaction();
        return owner.named().getJdbcTemplate();
    }

    static Params params() {
        return new Params();
    }
}
