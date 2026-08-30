package dev.wiggle.server.coord;

/**
 * A request to provision a namespace into its own cell: which storage to run on, how many nodes, the
 * region, and the base port the cluster listens on (node {@code i} uses {@code basePort + i}). This is
 * the input to {@link NamespaceProvisioner#create}; the durable record it produces is
 * {@link CoordNamespace}.
 */
public record NamespaceSpec(String namespace, StorageConfig storage, int replicas, String region, int basePort) {

    public NamespaceSpec {
        if (namespace == null || namespace.isBlank()) throw new IllegalArgumentException("namespace is required");
        if (storage == null) throw new IllegalArgumentException("storage is required");
        if (replicas < 1) replicas = 1;
    }

    /** A single-node in-memory cell -- the simplest spec (tests, single-box dev). */
    public static NamespaceSpec inMemory(String namespace, int basePort) {
        return new NamespaceSpec(namespace, StorageConfig.inMemory(), 1, null, basePort);
    }
}
