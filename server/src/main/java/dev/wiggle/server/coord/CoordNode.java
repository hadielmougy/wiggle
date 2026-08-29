package dev.wiggle.server.coord;

/** A server node registered with the coordinator (for discovery / health). {@code cellId} groups
 *  nodes that share a database into one cell (a namespace may span several cells via the ring). */
public record CoordNode(String id, String namespace, String cellId, String endpoint, String region,
                        String engineVersion, long configGeneration, long lastHeartbeat) {}
