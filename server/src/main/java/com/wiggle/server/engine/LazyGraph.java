package com.wiggle.server.engine;

import com.wiggle.core.Node;

import java.util.Optional;

public interface LazyGraph {
    String name();

    int version();

    String key();

    String startNode();

    Node node(String id);

    /** {@link #node}, for an id that may not be in the graph. */
    Optional<Node> find(String id);
}
