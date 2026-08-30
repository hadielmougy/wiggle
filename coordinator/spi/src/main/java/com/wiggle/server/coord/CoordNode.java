package com.wiggle.server.coord;

/** A server node registered with the coordinator (for discovery / health). {@code cellId} groups
 *  nodes that share a database into one cell (a namespace may span several cells via the ring);
 *  {@code cellFingerprint} is the stable identity of that shared storage (null when unknown), used to
 *  reject two distinct cells that reuse a {@code cellId}. */
public record CoordNode(String id, String namespace, String cellId, String endpoint, String region,
                        String engineVersion, String cellFingerprint, long configGeneration, long lastHeartbeat) {}
