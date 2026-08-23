package dev.wiggle.server.store;

import dev.wiggle.core.Node;
import dev.wiggle.core.WorkflowDefinition;

import java.util.List;
import java.util.Optional;

/**
 * Persistence for workflow definitions and their compiled graphs -- the engine's immutable,
 * content-addressed <em>reference data</em>, as distinct from the mutable runtime state served by
 * {@link Tx} (instances, tokens, leases, schedules, cluster nodes).
 *
 * <p>Because a version is a content hash, every write here is effectively write-once and
 * idempotent, and a read never has to be transactionally consistent with a runtime mutation -- the
 * graph for a given {@code (name, version)} never changes. Two representations are stored per
 * registration:
 * <ul>
 *   <li>the submitted definition as a JSON <b>blob</b> ({@link #putDefinition}) -- the source of
 *       truth for audit/describe, loaded whole only on cold admin paths;</li>
 *   <li>a <b>normalised</b> set of per-node/per-edge rows ({@link #putGraph}) the runtime reads one
 *       node at a time ({@link #graphNode}), so a huge topology is never materialised whole just to
 *       advance a single token.</li>
 * </ul>
 *
 * <p>{@link Tx} extends this interface so one transaction can span graph reads and runtime writes,
 * but a consumer that only walks the graph (e.g. the lazy graph view, the definition registry)
 * should depend on this narrower type -- it states, in the type, that it cannot touch runtime state.
 */
public interface GraphStore {

    // -- definition blob: source of truth for audit / describe --

    void putDefinition(String name, int version, String json);

    Optional<String> definition(String name, int version);

    /** Most recently registered version for a name. */
    Optional<Integer> latestVersion(String name);

    List<String> definitionNames();

    // -- normalised graph rows: the execution read path --

    /**
     * Normalises a definition's graph into per-node and per-edge rows so the runtime can
     * fetch a single node's neighbourhood without materialising the whole graph. Idempotent:
     * the version is a content hash, so re-registering the same graph is a no-op.
     */
    void putGraph(WorkflowDefinition def);

    /** One node plus its outgoing edges, reconstructed from the normalised rows. */
    Optional<Node> graphNode(String workflow, int version, String nodeId);

    /** The graph's entry node, without loading any other node. */
    Optional<String> graphStartNode(String workflow, int version);
}
