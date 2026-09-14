package com.wiggle.postgres;

import com.wiggle.jdbc.Dialect;

/**
 * H2 in PostgreSQL-compatibility mode: the embedded database used for development and the test
 * suite, not a deployment target. It takes the store's DDL and {@code ON CONFLICT} syntax verbatim,
 * so it inherits every {@link Dialect} default and overrides nothing. What it does not inherit is
 * the two capabilities PostgreSQL declares: it has no {@code FOR UPDATE SKIP LOCKED} and no
 * {@code RETURNING}, so the task claim falls back to compare-and-set, and no cheap cross-node
 * migration lock, which is moot because development and tests are single-node.
 *
 * <p>So this class is now only a name for "the defaults, and none of the capabilities". That is the
 * point rather than an accident: it is what makes H2 a faithful stand-in for the suite -- every
 * statement it runs is the statement PostgreSQL will run.
 */
public final class H2Dialect implements Dialect {

    @Override public String id() { return "h2"; }
}
