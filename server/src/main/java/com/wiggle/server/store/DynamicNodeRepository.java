package com.wiggle.server.store;

import com.wiggle.core.Node;

import java.util.List;
import java.util.Optional;

/**
 * Nodes an instance created at run time. Held on an instance shard.
 */
public interface DynamicNodeRepository {

    /** Writes the nodes an instance's created branches compiled to. They never change once written
     *  and are purged with the instance. */
    void insertDynNodes(String instanceId, List<Node> nodes);

    /** A node an instance created at run time, by its id; empty for any other id. */
    Optional<Node> dynNode(String nodeId);

    /** How many nodes an instance has created at run time. */
    long dynNodeCount(String instanceId);
}
