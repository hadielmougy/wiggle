package com.wiggle.placement;

import java.util.Optional;

/**
 * Where a namespace's placement is kept. The module owns the rules; this is how a host supplies the
 * state. The decisions stay in {@link Placements} and {@link Epochs}, which read and write only
 * through here.
 *
 * <p>Several coordinators may share one store, so {@link #compareAndSet} is the only write: read a
 * policy with its revision, compute the next one, write it back against that revision. An
 * implementation that ignores the revision and always writes will appear to work and will silently
 * lose an epoch under contention.
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
