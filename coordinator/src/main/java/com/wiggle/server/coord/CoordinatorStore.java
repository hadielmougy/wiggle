package com.wiggle.server.coord;

import com.wiggle.election.ElectionStore;

import java.util.List;
import java.util.Optional;

/**
 * Durable coordinator state: per-namespace placement policy, the node roster, and the definition
 * registry. Every policy mutation is a compare-and-set on the policy revision, so the tolerated
 * brief-overlap leader election (a stale ex-leader) cannot corrupt policy -- see
 * {@code docs/phase-1-tickets.md} (T5, T6). This is bounded state, never per-instance.
 */
public interface CoordinatorStore extends AutoCloseable {


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


    void upsertNode(CoordNode node);

    Optional<CoordNode> node(String id);

    List<CoordNode> nodes(String namespace);

    /** Updates a node's heartbeat + observed generation; returns the updated node, or empty if unknown. */
    Optional<CoordNode> touchNode(String id, long lastHeartbeat, long configGeneration);

    void removeNode(String id);

    /** Removes nodes whose last heartbeat is older than {@code deadlineMillis}. Returns the count removed. */
    int expireNodes(long deadlineMillis);


    /**
     * Atomically claim the binding {@code (namespace, cellId) -> fingerprint}, so two distinct cells cannot
     * register under one cell id even under a concurrent race. Returns {@code true} if the binding is held
     * by {@code fingerprint} afterwards -- newly claimed, or already equal (a replica of the same cell) --
     * and {@code false} if a <em>different</em> non-null fingerprint already holds it. A {@code null}
     * fingerprint is a no-op that returns {@code true} (the guard is skipped for a backend/node with no
     * storage identity).
     *
     * <p>This is the race-free replacement for a check-then-insert over the node roster: it is a
     * single-key claim (JDBC unique PK / Cassandra single-partition LWT / etcd txn / in-memory compute),
     * so it is atomic on every backend -- a roster scan cannot be.
     */
    boolean bindCell(String namespace, String cellId, String fingerprint);

    /**
     * Deletes cell bindings that no live node references any more (a cell that fully drained), so a
     * decommissioned cell id can later be reused by a genuinely new cell. Best-effort housekeeping run by
     * the leader's reconcile loop. Returns the count pruned.
     */
    int pruneOrphanCellBindings();


    Optional<CoordDefinition> getDefinition(String namespace, String name);

    /** Idempotent upsert keyed by (namespace, name). */
    void putDefinition(CoordDefinition def);

    /** Removes a workflow's allocation from a namespace; returns whether a row was removed. */
    boolean removeDefinition(String namespace, String name);

    List<CoordDefinition> definitions(String namespace);


    Optional<CoordNamespace> getNamespace(String namespace);

    List<CoordNamespace> namespaces();

    /** Idempotent upsert keyed by namespace; drives the provisioning state machine's persistence. */
    void putNamespace(CoordNamespace ns);


    /**
     * The roster of <em>coordinator processes</em>, for {@link com.wiggle.election.LeaderElection} --
     * the same announce-and-heartbeat election the cell engine runs, so there is one scheme to reason
     * about rather than two. Note this is a different roster from {@link #nodes(String)} above, which
     * holds the <em>cells</em> that report to this coordinator.
     *
     * <p>Leader-only duties here are the reconcile/retire loop, which is idempotent and re-entrant --
     * which is what makes an election without consensus sound.
     */
    ElectionStore election();

    @Override default void close() { }
}
