package dev.wiggle.server.coord;

import java.util.List;
import java.util.Optional;

/**
 * Durable coordinator state: per-namespace placement policy, the node roster, and the definition
 * registry. Every policy mutation is a compare-and-set on the policy revision, so the tolerated
 * brief-overlap leader election (a stale ex-leader) cannot corrupt policy -- see
 * {@code docs/phase-1-tickets.md} (T5, T6). This is bounded state, never per-instance.
 */
public interface CoordinatorStore extends AutoCloseable {

    // ---- policy (CAS-guarded) ----

    Optional<CoordPolicy> getPolicy(String namespace);

    List<CoordPolicy> listPolicies();

    /**
     * Compare-and-set the policy for {@code namespace}.
     *
     * <ul>
     *   <li>{@code expectedRevision == 0}: create; fails if a policy already exists.</li>
     *   <li>otherwise: update only if the stored revision equals {@code expectedRevision}.</li>
     * </ul>
     *
     * The new revision written is {@code expectedRevision + 1}; {@code desired.revision()} is ignored.
     *
     * @return the new revision on success, or {@code -1} if the CAS lost (stale revision, or a create
     * that raced an existing row).
     */
    long casPolicy(String namespace, long expectedRevision, CoordPolicy desired);

    // ---- node roster ----

    void upsertNode(CoordNode node);

    Optional<CoordNode> node(String id);

    List<CoordNode> nodes(String namespace);

    /** Updates a node's heartbeat + observed generation; returns the updated node, or empty if unknown. */
    Optional<CoordNode> touchNode(String id, long lastHeartbeat, long configGeneration);

    void removeNode(String id);

    /** Removes nodes whose last heartbeat is older than {@code deadlineMillis}. Returns the count removed. */
    int expireNodes(long deadlineMillis);

    // ---- definition registry (R23) ----

    Optional<CoordDefinition> getDefinition(String namespace, String name);

    /** Idempotent upsert keyed by (namespace, name). */
    void putDefinition(CoordDefinition def);

    List<CoordDefinition> definitions(String namespace);

    @Override default void close() { }
}
