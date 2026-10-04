package com.wiggle.server.store;

import com.wiggle.core.Node;

import java.util.List;
import java.util.Optional;

/** The reads of {@link GraphStore}: what a read-only connection can serve of the definitions. */
public interface GraphReads {

    Optional<String> definition(String name, int version);

    /** Highest registered version for a name. Versions are author-declared and ordered, so the
     *  newest is the largest -- not the most recently written. */
    Optional<Integer> latestVersion(String name);

    List<String> definitionNames();

    /** One node plus its outgoing edges, reconstructed from the normalised rows. */
    Optional<Node> graphNode(String workflow, int version, String nodeId);

    /** The graph's entry node, without loading any other node. */
    Optional<String> graphStartNode(String workflow, int version);

    /** How many nodes the graph has, without loading any; 0 when it is not registered. */
    int graphNodeCount(String workflow, int version);
}
