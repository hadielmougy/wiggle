package com.wiggle.server.engine;

import com.wiggle.core.Node;

public interface LazyGraph {
    String name();

    int version();

    String key();

    String startNode();

    Node node(String id);
}
