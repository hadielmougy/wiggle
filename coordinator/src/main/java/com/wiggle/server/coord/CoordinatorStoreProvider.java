package com.wiggle.server.coord;

/**
 * Implemented by a storage adapter that can also serve the coordinator's durable state over the same
 * backend (e.g. {@code JdbcStorage}, {@code CassandraStorage}). It is deliberately <em>separate</em> from
 * the engine's {@code Storage} interface: the cell engine must never know about coordinators, so the
 * composition layer ({@code dist}) resolves the coordinator store via this seam rather than via
 * {@code Storage}. The coordinator runtime and the engine share nothing but the gRPC contract.
 */
public interface CoordinatorStoreProvider {

    /** A coordinator store over this backend (the {@code coord_*} schema), sharing its connection/session. */
    CoordinatorStore coordinatorStore();
}
