package dev.wiggle.dist.coord;

/** No coordinator configured: every call is a no-op, so the standalone path is unchanged. */
public final class NoopCoordinatorLink implements CoordinatorLink {
    @Override public void register(NodeInfo node, CellRuntime runtime) { }
    @Override public void heartbeat() { }
    @Override public void close() { }
}
