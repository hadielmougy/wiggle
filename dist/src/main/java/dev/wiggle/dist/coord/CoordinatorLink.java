package dev.wiggle.dist.coord;

/**
 * A node's optional link to the cell coordinator: announce this node, keep it live, and deregister on
 * shutdown. Every method is best-effort -- a coordinator that is absent, down, or flapping must never
 * stop the node from serving. {@link NoopCoordinatorLink} is used when no coordinator is configured.
 */
public interface CoordinatorLink extends AutoCloseable {

    /** Announce this node to the coordinator (for discovery/health). */
    void register(NodeInfo node);

    /** Liveness ping; observing a newer config generation is how change propagates (T7). */
    void heartbeat();

    /** Deregister on shutdown. */
    @Override void close();

    /** What a node announces about itself. */
    record NodeInfo(String nodeName, String namespace, String endpoint, String engineVersion) {}
}
