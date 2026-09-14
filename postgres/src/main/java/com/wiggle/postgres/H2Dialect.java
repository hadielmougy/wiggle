package com.wiggle.postgres;

import com.wiggle.jdbc.Dialect;

/**
 * H2 in PostgreSQL-compatibility mode: the embedded database used for development and the test
 * suite, not a deployment target. It takes the store's DDL and {@code ON CONFLICT} syntax verbatim,
 * so it inherits the {@link Dialect} defaults; what it cannot do is {@code FOR UPDATE SKIP LOCKED}
 * or {@code RETURNING}, so the task claim falls back to compare-and-set. It has no cross-node
 * migration lock either, which is moot: development and tests are single-node.
 */
public final class H2Dialect implements Dialect {

    @Override public String id() { return "h2"; }

    @Override public String firstRow() { return "FETCH FIRST 1 ROWS ONLY"; }
}
