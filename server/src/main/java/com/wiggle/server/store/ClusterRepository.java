package com.wiggle.server.store;

import com.wiggle.server.store.Rows.ServerNode;

/**
 * Cluster membership and leadership: the writes, and the reads that must see the primary. Held on
 * the home shard.
 */
public interface ClusterRepository extends ClusterReads {

    void upsertNode(ServerNode node);

    void deleteNodesOlderThan(long lastHeartbeatBefore);

    void setLeader(String nodeId, boolean leader);
}
