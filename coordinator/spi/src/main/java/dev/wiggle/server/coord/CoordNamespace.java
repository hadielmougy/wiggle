package dev.wiggle.server.coord;

/**
 * The durable registry record for a provisioned namespace: its lifecycle {@link ProvisionState}, the
 * storage it was placed on, its size/region, the resolved cell endpoint once ACTIVE, and the last
 * error if it FAILED. Bounded state (one row per namespace), never per-instance.
 */
public record CoordNamespace(String namespace, ProvisionState state, StorageConfig storage,
                             int replicas, String region, String endpoint, String error, long updatedAt) {

    /** A fresh record in {@link ProvisionState#REQUESTED} from a spec. */
    public static CoordNamespace requested(NamespaceSpec spec, long now) {
        return new CoordNamespace(spec.namespace(), ProvisionState.REQUESTED, spec.storage(),
                spec.replicas(), spec.region(), null, null, now);
    }

    public CoordNamespace withState(ProvisionState next, long now) {
        return new CoordNamespace(namespace, next, storage, replicas, region, endpoint, null, now);
    }

    public CoordNamespace active(String endpoint, long now) {
        return new CoordNamespace(namespace, ProvisionState.ACTIVE, storage, replicas, region, endpoint, null, now);
    }

    public CoordNamespace failed(String error, long now) {
        return new CoordNamespace(namespace, ProvisionState.FAILED, storage, replicas, region, endpoint, error, now);
    }
}
