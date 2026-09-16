package com.wiggle.election;

import java.util.List;
import java.util.function.Function;

/**
 * The durable roster an election runs on. Implemented once per side -- over the cell's node table,
 * over the coordinator's -- so the election rule lives in one place.
 *
 * <p>One method, and it must run in a single transaction: split into "upsert, then read, then write
 * the verdict" and two nodes can interleave and reach different answers from different snapshots.
 */
public interface ElectionStore {

    /**
     * One election step, atomically. The implementation must, in a single transaction:
     *
     * <ol>
     *   <li>upsert {@code self} (its {@code lastHeartbeat} is already set to now);</li>
     *   <li>delete rows whose {@code lastHeartbeat} is older than {@code pruneBefore} -- long-dead
     *       processes, not merely late ones, so the table does not grow without bound;</li>
     *   <li>read the roster;</li>
     *   <li>call {@code elect} with it and persist the verdict it returns (the winning id, or null
     *       when the roster has no live member) however this backend surfaces it;</li>
     *   <li>return the roster that {@code elect} was given.</li>
     * </ol>
     *
     * <p>Persisting the verdict is for readers -- a dashboard, an operator -- not for the election:
     * nothing reads it back to decide anything, so a backend with nowhere to put it may drop it.
     */
    List<Member> step(Member self, long pruneBefore, Function<List<Member>, String> elect);

    /**
     * Backdate {@code self}'s heartbeat to zero and clear any leader flag, so peers re-elect at once
     * rather than waiting out the full timeout. Best-effort: called on a graceful shutdown, and the
     * timeout covers the ungraceful case.
     */
    void standDown(Member self);

    /** The roster as it stands, for status and reporting. Not part of the election. */
    List<Member> members();
}
