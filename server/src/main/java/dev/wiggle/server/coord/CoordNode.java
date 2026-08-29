package dev.wiggle.server.coord;

/** A server node registered with the coordinator (for discovery / health). */
public record CoordNode(String id, String namespace, String endpoint, String region,
                        String engineVersion, long configGeneration, long lastHeartbeat) {}
