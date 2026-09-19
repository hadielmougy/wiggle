package com.wiggle.server.engine;

/**
 * Where a new instance's id comes from: the legacy {@code wfi_...} form by default, or the
 * epoch-aware {@code ns[.c{cell}].e{epoch}.s{shard}.ulid} form ({@link com.wiggle.core.IdCodec})
 * once the cell is placed under a coordinator.
 *
 * <p>An id is minted per start, not per instance object, so an implementation must be safe to call
 * from several threads and must not reuse a value.
 */
@FunctionalInterface
public interface InstanceIds {

    /** A fresh, never-before-issued instance id. */
    String next();
}
