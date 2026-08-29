package dev.wiggle.server.coord;

import dev.wiggle.server.ServerConfig;
import dev.wiggle.server.ServerRole;
import dev.wiggle.server.WiggleServer;
import dev.wiggle.server.store.InMemoryStorage;
import dev.wiggle.server.store.Storage;
import dev.wiggle.server.store.StorageFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link CellDeployer} that runs cells <em>in-process</em>: it builds each node's {@link ServerConfig}
 * from the spec (resolving the storage secret at deploy time) and starts a {@link WiggleServer} per
 * replica. Intended for a single box, tests, or a pod entrypoint that is itself the cell. The forked
 * variant is {@link ProcessCellDeployer}; a container/pod deployer is a later impl of the same seam.
 *
 * <p>This is the only place the coordinator side touches {@code StorageFactory} / {@code WiggleServer}
 * (R22). Started cells are tracked by deployment id (the namespace) so {@link #teardown} and
 * {@link #close} can stop them.
 */
public final class EmbeddedCellDeployer implements CellDeployer {

    private static final System.Logger LOG = System.getLogger(EmbeddedCellDeployer.class.getName());

    private final ServerConfig template;
    private final StorageFactory factory;
    private final SecretResolver secrets;
    private final Map<String, List<WiggleServer>> deployments = new ConcurrentHashMap<>();

    public EmbeddedCellDeployer(ServerConfig template, StorageFactory factory, SecretResolver secrets) {
        this.template = template;
        this.factory = factory;
        this.secrets = secrets == null ? SecretResolver.ENV : secrets;
    }

    /** In-memory single-box deployer for tests/dev: defaults template, in-memory storage, env secrets. */
    public static EmbeddedCellDeployer inMemory() {
        return new EmbeddedCellDeployer(ServerConfig.fromEnvironment(),
                cfg -> new InMemoryStorage(), SecretResolver.ENV);
    }

    @Override public void migrateSchema(NamespaceSpec spec) {
        // Migrate the shared schema once, before any node serves, so a joining node never races an
        // unmigrated database (R23 at the schema level). Idempotent: the baseline guard makes a repeat a
        // no-op, and each node's own start migrates idempotently too.
        Storage storage = factory.create(cellConfig(spec, spec.basePort()));
        try {
            storage.migrate(ServerRole.CELL);
        } finally {
            storage.close();
        }
    }

    @Override public Deployment deploy(NamespaceSpec spec) {
        List<WiggleServer> existing = deployments.get(spec.namespace());
        if (existing != null && !existing.isEmpty()) {
            return new Deployment(spec.namespace(), existing.get(0).baseUrl());   // idempotent resume
        }
        int replicas = spec.replicas();
        if (replicas > 1 && spec.storage().isInMemory()) {
            LOG.log(System.Logger.Level.WARNING, () -> "in-memory cell '" + spec.namespace()
                    + "' cannot share a store across nodes; deploying a single node");
            replicas = 1;
        }
        List<WiggleServer> started = new ArrayList<>();
        try {
            for (int i = 0; i < replicas; i++) {
                started.add(new WiggleServer(cellConfig(spec, spec.basePort() + i), factory).start());
            }
        } catch (IOException e) {
            for (WiggleServer s : started) s.close();   // roll back a partial deploy
            throw new UncheckedIOException("deploy of cell '" + spec.namespace() + "' failed", e);
        }
        deployments.put(spec.namespace(), started);
        return new Deployment(spec.namespace(), started.get(0).baseUrl());
    }

    @Override public void teardown(String deploymentId) {
        List<WiggleServer> servers = deployments.remove(deploymentId);
        if (servers == null) return;
        for (WiggleServer s : servers) s.close();
    }

    @Override public void close() {
        for (String id : List.copyOf(deployments.keySet())) teardown(id);
    }

    private ServerConfig cellConfig(NamespaceSpec spec, int port) {
        StorageConfig sc = spec.storage();
        return template
                .withRole(ServerRole.CELL)
                .withNamespace(spec.namespace())
                .withStorage(sc.jdbcUrl(), sc.user(), secrets.resolve(sc.secretRef()), Math.max(1, sc.poolSize()))
                .withPort(port);
    }
}
