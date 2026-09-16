package com.wiggle.placement;

import java.util.Optional;

/**
 * Where a namespace's placement is kept. The module owns the <em>rules</em>; this is how a host
 * supplies the <em>state</em>.
 *
 * <p>Deliberately narrow: two methods, no transactions, no connection handling, nothing about how
 * a policy is encoded. It is the set the rules actually use -- listing and deleting namespaces are
 * host concerns, and adding them here because they sounded natural would be inventing a contract
 * for implementors to satisfy and nothing to call. A coordinator backed by PostgreSQL, an in-memory reference implementation and
 * a test double are all a few lines each, and none of them can accidentally take a decision — the
 * decisions are in {@link Placements} and {@link Epochs}, which only ever read and write through
 * this interface.
 *
 * <p><b>Concurrency is the whole contract.</b> Several coordinators may share one store, so
 * {@link #compareAndSet} is the only write. A caller reads a policy with its revision, computes the
 * next one, and writes it back against the revision it read; a losing writer is told so and retries
 * from a fresh read. An implementation that ignores the revision and always writes will
 * <em>appear</em> to work and will silently lose an epoch under contention, which is the one bug
 * this interface exists to make impossible to write by accident.
 */
public interface PlacementStore {

    /** A policy and the revision it was read at, for the compare-and-set that follows. */
    record Versioned(Ring.Policy policy, long revision) {}

    /** The current policy for a namespace, or empty when it has never been placed. */
    Optional<Versioned> get(String namespace);

    /**
     * Writes {@code updated} only if the stored revision is still {@code expectedRevision}. Use
     * {@code 0} for "must not exist yet".
     *
     * @return true when the write landed; false when someone else got there first
     */
    boolean compareAndSet(String namespace, long expectedRevision, Ring.Policy updated);

}
