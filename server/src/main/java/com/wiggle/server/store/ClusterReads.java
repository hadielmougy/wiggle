package com.wiggle.server.store;

import com.wiggle.server.store.Rows.ServerNode;

import java.util.List;

/**
 * Cluster membership and leadership: the reads, which a read-only connection such as a read
 * replica can serve. Held on the home shard.
 */
public interface ClusterReads {

    List<ServerNode> nodes();
}
