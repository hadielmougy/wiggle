package dev.wiggle.server.coord;

/**
 * Drives a namespace from a {@link NamespaceSpec} to a running cell through the {@link CellDeployer}
 * seam, recording every step in the {@link CoordinatorStore} namespace registry:
 * {@code REQUESTED → MIGRATING_SCHEMA → STARTING → ACTIVE}. Schema is migrated once, before any node
 * serves (R23). The coordinator never touches {@code StorageFactory}/{@code WiggleServer} directly --
 * only the deployer does (R22).
 *
 * <p>{@link #create} is idempotent and resumable: an already-ACTIVE namespace is returned untouched,
 * and any step that throws leaves a {@link ProvisionState#FAILED} record whose next {@code create} runs
 * the remaining steps again (migrate + deploy are idempotent).
 */
public final class NamespaceProvisioner {

    private static final System.Logger LOG = System.getLogger(NamespaceProvisioner.class.getName());

    private final CoordinatorStore store;
    private final CellDeployer deployer;

    public NamespaceProvisioner(CoordinatorStore store, CellDeployer deployer) {
        this.store = store;
        this.deployer = deployer;
    }

    /** Provisions (or resumes provisioning of) {@code spec}'s namespace; returns the resulting record. */
    public CoordNamespace create(NamespaceSpec spec) {
        CoordNamespace rec = store.getNamespace(spec.namespace())
                .orElseGet(() -> persist(CoordNamespace.requested(spec, now())));
        if (rec.state() == ProvisionState.ACTIVE) return rec;   // already provisioned -> no-op
        try {
            rec = persist(rec.withState(ProvisionState.MIGRATING_SCHEMA, now()));
            deployer.migrateSchema(spec);

            rec = persist(rec.withState(ProvisionState.STARTING, now()));
            CellDeployer.Deployment d = deployer.deploy(spec);

            CoordNamespace active = persist(rec.active(d.endpoint(), now()));
            LOG.log(System.Logger.Level.INFO, () -> "namespace '" + spec.namespace()
                    + "' ACTIVE at " + active.endpoint());
            return active;
        } catch (RuntimeException e) {
            CoordNamespace failed = persist(rec.failed(String.valueOf(e.getMessage()), now()));
            LOG.log(System.Logger.Level.WARNING,
                    "provisioning '" + spec.namespace() + "' failed at " + rec.state() + " (resumable): " + e);
            return failed;
        }
    }

    private CoordNamespace persist(CoordNamespace rec) {
        store.putNamespace(rec);
        return rec;
    }

    private static long now() {
        return System.currentTimeMillis();
    }
}
