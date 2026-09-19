package com.wiggle.server.store;

import com.wiggle.core.Node;
import com.wiggle.core.WorkflowDefinition;

import java.util.List;
import java.util.Optional;

/**
 * Persistence for workflow definitions and their compiled graphs -- the engine's immutable
 * <em>reference data</em>, as distinct from the mutable runtime state served by {@link Tx}
 * (instances, tokens, leases, schedules, cluster nodes).
 *
 * <p>A published {@code (name, version)} is write-once: the registry refuses to redefine one whose
 * stored fingerprint differs, so a read never has to be transactionally consistent with a runtime
 * mutation -- the graph for a given {@code (name, version)} never changes under a running instance.
 * Two representations are stored per registration:
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

    /** Writes the blob for a version that does not exist yet. Idempotent: an existing row is left
     *  alone, so two nodes registering the same new version cannot collide. */
    void putDefinition(String name, int version, String json, String fingerprint, String fingerprintAlgo);

    /** Overwrites the blob of an existing version. Only reached once
     *  {@link com.wiggle.server.engine.DefinitionRegistry} has allowed the replacement, under the
     *  row lock {@link #definitionFingerprint} took. */
    void replaceDefinition(String name, int version, String json, String fingerprint, String fingerprintAlgo);

    Optional<String> definition(String name, int version);

    /** The stored fingerprint and the algorithm that produced it, or empty if this version is new. */
    Optional<StoredFingerprint> definitionFingerprint(String name, int version);

    /** What the store remembers about a registered version's graph identity. A null {@code value}
     *  is a row written before fingerprints existed: unknown, not mismatched. */
    record StoredFingerprint(String value, String algo) {}

    /** Highest registered version for a name. Versions are author-declared and ordered, so the
     *  newest is the largest -- not the most recently written. */
    Optional<Integer> latestVersion(String name);

    List<String> definitionNames();

    // -- normalised graph rows: the execution read path --

    /**
     * Normalises a definition's graph into per-node and per-edge rows so the runtime can
     * fetch a single node's neighbourhood without materialising the whole graph. Idempotent for an
     * unchanged graph; {@link #deleteGraph} first when replacing one.
     */
    void putGraph(WorkflowDefinition def);

    /** Drops a version's normalised rows, for the forced replacement of a registered graph. */
    void deleteGraph(String workflow, int version);

    /** One node plus its outgoing edges, reconstructed from the normalised rows. */
    Optional<Node> graphNode(String workflow, int version, String nodeId);

    /** The graph's entry node, without loading any other node. */
    Optional<String> graphStartNode(String workflow, int version);
}
