package com.wiggle.server.store;

import com.wiggle.server.store.Rows.Instance;

import java.util.List;
import java.util.Optional;

/**
 * Workflow instances: the writes, and the reads that must see the primary. Held on an instance
 * shard.
 */
public interface InstanceRepository extends InstanceReads {

    void insertInstance(Instance instance);

    /** Acquires the instance write-lock for the remainder of this transaction. */
    Optional<Instance> lockInstance(String id);

    /**
     * {@code lockInstance} for a set of rows, {@code ids} already sorted ascending. The default
     * loops -- exactly today's one-lock-per-statement, in the caller's order. A JDBC backend
     * overrides it with one {@code WHERE id IN (...) ORDER BY id FOR UPDATE}: on a btree primary
     * key the scan yields ascending ids with no separate sort node, so rows lock in id order, and
     * two overlapping statements of this same shape acquire in the same global order -- no cycle.
     * Were a planner ever to reorder, the deadlock is detected by the database and surfaces as
     * the throw the batch's replay path already handles. Missing ids are simply absent.
     */
    default List<Instance> lockInstances(List<String> ids) {
        List<Instance> out = new java.util.ArrayList<>(ids.size());
        for (String id : ids) lockInstance(id).ifPresent(out::add);
        return out;
    }

    /**
     * Writes back the fields of an instance that change as it runs: status, termination reason,
     * error and context, stamped with {@code updatedAt}.
     *
     * <p>It writes nothing else. An instance's identity and provenance -- {@code workflow},
     * {@code version}, {@code correlationId}, {@code parentTokenId}, {@code createdAt} -- are
     * settled when the row is inserted and are not reachable from here, so a body that hands back
     * a row carrying a changed one cannot rewrite history with it. Nothing in the engine assigns
     * them after birth; this is what keeps that true of the store as well.
     *
     * <p>The revision is advanced by the store, from the value the row holds rather than from the
     * caller's: it counts the writes the row has taken. A caller working from a stale copy is
     * therefore not able to walk the revision backwards, and its own copy is advanced from where
     * it was so the engine can write the row on again.
     */
    void updateInstance(Instance instance);

    /** {@code updateInstance} for a set of rows; same contract as {@link #insertTokens}. */
    default void updateInstances(List<Instance> instances) {
        for (Instance i : instances) updateInstance(i);
    }

    int deleteTerminalInstancesBefore(long updatedBefore, int limit);
}
